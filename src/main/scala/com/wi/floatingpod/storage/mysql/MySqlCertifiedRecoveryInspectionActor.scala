package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.storage.PhysicalRecoveryBinding
import MySqlCertifiedOrdinaryCapabilities.{OrdinaryNativeHandoffAcknowledgement, OrdinaryNativeSettledAcknowledgement}
import com.wi.floatingpod.server.{B1ResumeRecord, B1ResumeRecordCanonical, B1ResumeRecordV5, WindowsB1TwoSlotStore, WindowsB1V5WitnessRecordStore, WindowsMarkerIo, WindowsWorldProvisioner}
import java.sql.{Connection, PreparedStatement, ResultSet, SQLDataException, SQLTransientConnectionException}
import java.time.LocalDateTime
import java.util.{Arrays, UUID}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.*

/** 未business-admitted的专用inspection；唯一Session先注册，原root不随读图或native结束退休。 */
private[floatingpod] final class MySqlCertifiedRecoveryInspectionActor private[mysql] (
    private val session: MySqlOwnedDatabaseSession, private val inputs: MySqlRuntimeInputs,
    private val ledger: MySqlFormat3AllocationLedger, val request: CertifiedSourceRequest):
  import MySqlCertifiedRecoveryInspectionActor.VerifiedV5ArtifactSource
  private val owner = inputs.ownedIoThread
  private var retainedExpectation = inputs.auditExpectation
  private var runtimeGraphDropped=false
  private[mysql] def expectation: MySqlSchemaAuditExpectation =
    if runtimeGraphDropped then throw new IllegalStateException("certified actor runtime graph is closed")
    retainedExpectation
  private val native = new MySqlCertifiedRestoreNativeHolder(owner, request)
  private var verified: VerifiedMySqlConnection = null
  private var scope: MySqlAuditBootScope = null
  private var held: HeldSessionBarrier = null
  private var qualification: VerifiedSchemaQualification = null
  private var root: MySqlFormat3ReadBudget = null
  private var parent: MySqlFull20ColdReadAccounting = null
  private val accountings = ArrayBuffer.empty[MySqlFull20ColdReadAccounting]
  private val commonDrafts = ArrayBuffer.empty[MySqlB1CommonGraphColdCodec.Draft]
  private var current: MySqlFormat3FullSnapshot = null
  private var rollbackOnly: MySqlRollbackOnlyState = null
  private var attempted = false
  private var primary = Option.empty[Throwable]
  private var proof: ColdOriginalComparisonProof = null
  private var result: MySqlCertifiedInspectionObservation | MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement | Null = null
  private var ordinarySource: MySqlCertifiedOrdinaryCapabilities.VerifiedOriginalCertifiedSource = null
  private var ordinaryPlan: MySqlCertifiedOrdinaryCapabilities.PlannedCertifiedHandoff = null
  private var ordinaryInstalledScope: com.wi.floatingpod.server.WindowsB1TwoSlotStore.InstalledResumeRecordScope = null
  private var ordinaryCommitted: MySqlCertifiedOrdinaryCapabilities.CommittedCertifiedHandoff = null
  private var ordinarySettledRecord: com.wi.floatingpod.server.B1ResumeRecord = null
  private var ordinaryCompleted: MySqlCertifiedOrdinaryCapabilities.CompletedCertifiedHandoff = null
  private var ordinaryDraft: MySqlOrdinaryHandoffGraph.Draft = null
  private var ordinaryWitnessDraft: MySqlFull20OriginalWitness.Draft = null
  private var ordinaryWitness: MySqlFull20OriginalWitness = null
  private var ordinaryTransfer: MySqlFull20NativeTransfer = null
  private var ordinarySettledNativeAcknowledgement: MySqlCertifiedOrdinaryCapabilities.OrdinaryNativeSettledAcknowledgement = null
  private var ordinaryStarted = false
  private var ordinaryClaimed = false
  private var recallRecovery = Option.empty[MySqlCertifiedRecoveryInspectionActor.RecallRecoveryAdmission]
  private final case class RecallConfirmedHandoff(data: MySqlOrdinaryHandoffGraph.ConfirmedHandoffData,
      before: MySqlFormat3FullSnapshot, target: MySqlFormat3FullSnapshot)
  private var recallHandoffs = Vector.empty[RecallConfirmedHandoff]
  private var recallSqlHistory = Vector.empty[MySqlNormalRecallRecovery.ReceiptHistoryData]
  private def connection = verified.connection
  private var quotedSchema = MySqlFencedTransactionSupport.quote(expectation.schema)
  private def inspectionRequest: MySqlCertifiedRestoreRequest = request match
    case CertifiedSourceRequest.Inspection(value) => value
    case _: CertifiedSourceRequest.Ordinary => throw new IllegalStateException("ordinary actor cannot enter InspectionOnly comparison")
  private def ordinaryRequest: MySqlOrdinaryCertifiedBootRequest = request match
    case CertifiedSourceRequest.Ordinary(value) => value
    case _: CertifiedSourceRequest.Inspection => throw new IllegalStateException("InspectionOnly actor cannot enter ordinary issuer")

  private[mysql] def attach(v: VerifiedMySqlConnection, s: MySqlAuditBootScope, h: HeldSessionBarrier, q: VerifiedSchemaQualification): Unit =
    session.requireRecoveryActor(this)
    require(verified == null && v != null && s != null && h != null && q != null && (s.ownerThread eq owner) &&
      s.verifiedConnection.contains(v), "exact registered inspection connection/scope/barrier required once")
    h.readmit(v, expectation, s)
    verified = v
    scope = s
    held = h
    qualification = q
    requireQualification()
    root = MySqlFormat3ReadBudget.rootOnLedger(inputs.format3Contract.limitProfile,
      inputs.format3Contract.limitProfile.maxSqlPacketBytes(), ledger)
    // OPEN path precedes a known payload length. Bound derives from native max UTF16
    // path(32766), diagnostic/path/hash/metadata buffers and parent registry wrappers.
    root.reserveRetainedMemory(202752L, "cold parent and serial native fixed OPEN workspace before allocation")
    parent = new MySqlFull20ColdReadAccounting(this, root, None)
    accountings += parent

  private def requireQualification(): Unit =
    require(qualification != null && qualification.isCurrent(inputs.bootId, inputs.bootGeneration,
      scope.connectionGeneration, expectation.serverUuid, expectation.schema, expectation.bindingUuid,
      expectation.profile, expectation.expectedPhysicalFormat, verified, owner, System.nanoTime()),
      "original exact certified schema qualification is unavailable or expired")

  private[mysql] def requireAccounting(value: MySqlFull20ColdReadAccounting, exact: MySqlFormat3ReadBudget): Unit =
    session.requireRecoveryActor(this)
    primary.foreach(throw _)
    if result.isInstanceOf[MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement] then
      throw new IllegalStateException("Ordinary accounting consumption has been sealed")
    require((Thread.currentThread() eq owner) && root != null && (exact eq root) && !root.isClosed &&
      accountings.exists(_ eq value), "accounting must belong to original registered actor root")
  private[mysql] def requireRegisteredLookupParent(value: MySqlFull20ColdReadAccounting,
      exact: MySqlFormat3ReadBudget): Unit =
    requireAccounting(value, exact)
    require(value eq parent, "exact original registered lookup parent accounting is required")
  private[mysql] def issueReadAccounting(p: MySqlFull20ColdReadAccounting): MySqlFull20ColdReadAccounting =
    requireAccounting(p, root)
    require((p eq parent) && accountings.size <= 6, "at most six registered cold native read children")
    root.reserveRetainedMemory(512L, "cold read child registry before allocation")
    val child = new MySqlFull20ColdReadAccounting(this, root, Some(parent))
    accountings += child
    child
  private[mysql] def issueHistoricalReadAccounting(p: MySqlFull20ColdReadAccounting,
      permit: com.wi.floatingpod.server.WindowsB1TwoSlotStore.Full20HistoricalDiscoveryPermit): MySqlFull20ColdReadAccounting =
    requireAccounting(p, root)
    require((p eq parent) && permit != null, "actual historical parent/permit required")
    p.requireHistoricalDiscovery(permit)
    root.reserveRetainedMemory(512L, "historical child accounting registry before allocation")
    val child = new MySqlFull20ColdReadAccounting(this, root, Some(parent))
    accountings += child
    child
  private[mysql] def issueV5HistoryReadAccounting(p: MySqlFull20ColdReadAccounting,
      lookup: com.wi.floatingpod.server.WindowsB1V5HistoricalLookup): MySqlFull20ColdReadAccounting =
    requireAccounting(p, root)
    require(p eq parent, "original registered V5 history parent required")
    p.requireV5History(lookup)
    root.reserveRetainedMemory(512L, "registered V5 history child before allocation")
    val child = new MySqlFull20ColdReadAccounting(this, root, Some(parent))
    accountings += child
    child

  private[mysql] def recordFailure(cause: Throwable): Unit =
    session.requireRecoveryActor(this)
    require(cause != null, "original certified actor failure required")
    primary = MySqlOwnedSessionLifecycle.combine(primary, cause)
    try
      if ordinaryCommitted != null then ordinaryCommitted.fail(cause)
      else if ordinaryPlan != null then ordinaryPlan.fail(cause)
      else if ordinarySource != null then ordinarySource.registerFailure(cause)
    catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, cleanup)
    if ordinaryStarted then
      try session.failCertifiedOrdinaryHandoff(this, cause)
      catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, cleanup)
  private[mysql] def rootRetained: Boolean = root != null
  private[mysql] def retainedFailure: Option[Throwable] = primary
  private[mysql] def retainedNativeResponsibility: Boolean = native.retainedResponsibility

  /** The V5 native boundary accepts this opaque permit only from this exact committed/post-audit actor. */
  private[mysql] def requireCommittedNativeContinuation(
      committed: MySqlCertifiedOrdinaryCapabilities.CommittedCertifiedHandoff,
      flight: com.wi.floatingpod.server.WindowsB1V5WitnessRecordStore.VerifiedHandoffRetirementFlight,
      installed: com.wi.floatingpod.server.WindowsB1TwoSlotStore.InstalledResumeRecordScope
  ): Unit =
    session.requireRecoveryActor(this)
    require((Thread.currentThread() eq owner) && committed != null && (ordinaryCommitted eq committed) &&
      ordinaryPlan != null && (committed.plan eq ordinaryPlan) && ordinarySource != null &&
      (committed.plan.source eq ordinarySource) && installed != null && (ordinaryInstalledScope eq installed) &&
      primary.isEmpty && ordinaryPlan.retainedFailure.isEmpty && root != null && !root.isClosed,
      "exact same actor/session/connection committed HANDOFF and post-audit state is required before native continuation")
    ordinaryPlan.requireCommitted()
    ordinarySource.requireFor(this, session, verified, scope, held)
    native.requireOrdinaryInstalledContinuation(installed, flight)

  /** A DB_ACQUIRED transition is accepted only when it is the actor's exact committed target. */
  private[mysql] def requireSettledNativeRecord(
      committed: MySqlCertifiedOrdinaryCapabilities.CommittedCertifiedHandoff,
      settled: com.wi.floatingpod.server.B1ResumeRecord
  ): Unit =
    session.requireRecoveryActor(this)
    require((Thread.currentThread() eq owner) && committed != null && (ordinaryCommitted eq committed) &&
      ordinarySettledRecord != null && settled != null &&
      Arrays.equals(com.wi.floatingpod.server.B1ResumeRecordCanonical.encode(ordinarySettledRecord),
        com.wi.floatingpod.server.B1ResumeRecordCanonical.encode(settled)),
      "native DB_ACQUIRED must use the exact actor-settled HANDOFF transition")

  private[mysql] def requireOriginalV4Predecessor(
      committed: MySqlCertifiedOrdinaryCapabilities.CommittedCertifiedHandoff,
      record: com.wi.floatingpod.server.B1ResumeRecord,
      canonical: Array[Byte], fileIdentity: String
  ): Unit =
    session.requireRecoveryActor(this)
    require((Thread.currentThread() eq owner) && committed != null && (ordinaryCommitted eq committed) &&
      ordinarySource != null && record != null && canonical != null && canonical.nonEmpty &&
      fileIdentity != null && fileIdentity.nonEmpty &&
      Arrays.equals(com.wi.floatingpod.server.B1ResumeRecordCanonical.encode(ordinarySource.oldCurrentV4.record()), canonical) &&
      Arrays.equals(com.wi.floatingpod.server.B1ResumeRecordCanonical.encode(ordinarySource.oldCurrentV4.record()),
        com.wi.floatingpod.server.B1ResumeRecordCanonical.encode(record)) &&
      Arrays.equals(ordinarySource.oldCurrentV4.canonicalBytes(), canonical) &&
      ordinarySource.oldCurrentV4.fileIdentity() == fileIdentity,
      "installed V4 retirement must target the exact actual predecessor bytes and native identity from original source inspection")

  private[mysql] def issueVerifiedV5ArtifactSource(
      reference: com.wi.floatingpod.server.B1Full20WitnessReference,
      expectedAttempt: com.wi.floatingpod.server.B1ResumeRecord.AttemptAssertions,
      identity: com.wi.floatingpod.server.WindowsMarkerIo.FileIdentity
  ): VerifiedV5ArtifactSource =
    session.requireRecoveryActor(this)
    require((Thread.currentThread() eq owner) && reference != null && expectedAttempt != null && identity != null &&
      root != null && !root.isClosed && primary.isEmpty, "exact live original actor/reference/identity required")
    val observation = native.coldObservation
    require(observation.status() == com.wi.floatingpod.server.WindowsB1ColdResumeLookup.Status.RESOLVED_DATA &&
      observation.currentRecord() != null, "actual resolved settled V5 cold observation required")
    val v5 = observation.currentRecord()
    require(v5.allReferences().asScala.exists(value => value != null && value.exactlyEquals(reference)),
      "exact full20 reference must be retained by the actual current V5 source")
    val observed = observation.artifacts().asScala.find(value => value.reference().exactlyEquals(reference))
      .getOrElse(throw new java.io.IOException("actual cold lookup artifact observation is missing"))
    require(observed.identity() == identity && observed.fullLength() == reference.fullLength() &&
      reference.fileIdentity() == identity &&
      Arrays.equals(reference.profileSha256(), root.profile.sha256()) &&
      reference.attemptId() == expectedAttempt.attemptId() && reference.phase() == expectedAttempt.phase() &&
      reference.logicalOperation() == expectedAttempt.logicalWriteUuid(),
      "full20 reference identity/length/profile/complete attempt differs from actual cold observation")
    root.reserveRetainedMemory(768L, "private V5 source artifact evidence holder before allocation")
    root.reserveTemporaryMemory(2L * 65536L, "exact complete attempt comparison buffers before allocation")
    var expectedBytes: Array[Byte] = null
    var actualBytes: Array[Byte] = null
    var primaryFailure: Throwable = null
    try
      expectedBytes = com.wi.floatingpod.server.B1ResumeRecordCanonical.encodeAttemptData(expectedAttempt)
      require(expectedBytes.nonEmpty && expectedBytes.length <= 65536, "complete bounded V5 attempt encoding required")
      val matches = accountings.iterator.filter(value => value.witness != null && value.retainedBytes != null &&
        value.retainedBytes.length.toLong == reference.fullLength()).filter { value =>
          val matched = value.matchedWitnessAttempt()
          if matched == null then false
          else
            actualBytes = com.wi.floatingpod.server.B1ResumeRecordCanonical.encodeAttemptData(matched)
            val equal = Arrays.equals(expectedBytes, actualBytes)
            Arrays.fill(actualBytes, 0.toByte); actualBytes = null
            equal
        }.toVector
      require(matches.size == 1, "one actual decoded actor-owned full20 payload must match the complete source attempt")
      val child = matches.head
      val payload = child.retainedBytes
      val witness = child.witness
      val digest = com.wi.floatingpod.server.WindowsWorldProvisioner.digest(payload)
      require(payload.length.toLong == reference.fullLength() && Arrays.equals(digest, reference.fullSha256()) &&
        Arrays.equals(witness.originalRecord.sourceAnchorSha256(), reference.sourceAnchorSha256()) &&
        Arrays.equals(com.wi.floatingpod.server.WindowsWorldProvisioner.digest(witness.originalRecord.originalReceipt()),
          reference.originalReceiptSha256()),
        "retained full20 payload digest/source anchor/receipt differs from complete reference DATA")
      new VerifiedV5ArtifactSource(this, reference, identity, child, payload, expectedAttempt)
    catch
      case cause: Throwable =>
        primaryFailure = cause
        throw cause
    finally
      if expectedBytes != null then Arrays.fill(expectedBytes, 0.toByte)
      if actualBytes != null then Arrays.fill(actualBytes, 0.toByte)
      try root.releaseTemporaryMemory(2L * 65536L, "complete source attempt comparison arrays dropped")
      catch
        case cleanup: Throwable =>
          if primaryFailure != null && (cleanup ne primaryFailure) then primaryFailure.addSuppressed(cleanup)
          else if primaryFailure == null then throw cleanup

  private def requireV5ArtifactSource(value: VerifiedV5ArtifactSource): Unit =
    session.requireRecoveryActor(this)
    require((Thread.currentThread() eq owner) && root != null && !root.isClosed && primary.isEmpty &&
      value != null && (value.actor.eq(this)) && accountings.exists(_ eq value.heldAccounting) &&
      (value.heldAccounting.retainedBytes eq value.heldPayload) && value.heldPayload.nonEmpty &&
      value.reference.fileIdentity() == value.identity && value.heldAttempt != null &&
      value.reference.attemptId() == value.heldAttempt.attemptId() &&
      value.reference.phase() == value.heldAttempt.phase() &&
      value.reference.logicalOperation() == value.heldAttempt.logicalWriteUuid() &&
      Arrays.equals(value.reference.fullSha256(), com.wi.floatingpod.server.WindowsWorldProvisioner.digest(value.heldPayload)),
      "exact actual full20 source payload capability is no longer current")
  private[mysql] def prepareOrdinaryExit(): MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement =
    session.requireRecoveryActor(this)
    if Thread.currentThread() ne owner then throw new IllegalStateException("original Ordinary worker required")
    if primary.nonEmpty then throw primary.get
    if !ordinaryClaimed || ordinaryCompleted == null || ordinaryTransfer == null || root == null || root.isClosed || result != null then
      throw new IllegalStateException("completed original Ordinary actor prepares exit once")
    root.reserveRetainedMemory(40L, "原Ordinary终结状态在构造前预付，失败留原root")
    val state = new MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement(root)
    result = state
    state

  private[mysql] def requireOrdinaryExitState(state: MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement): Unit =
    session.requireRecoveryActor(this)
    if (Thread.currentThread() ne owner) || state == null || (result ne state) || (state.originalRoot ne root) then
      throw new IllegalStateException("exact original Ordinary actor terminal state required")
    if state.failure != null then throw state.failure
    if primary.nonEmpty then throw primary.get

  private[mysql] def ordinaryNativeExitPermit(state: MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement): MySqlCertifiedOrdinaryNativeExitPermit =
    requireOrdinaryExitState(state)
    native.issueOrdinaryExitPermit(session, this, root, state)

  private[mysql] def detachOrdinaryBusinessConsumers(state: MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement): Unit =
    requireOrdinaryExitState(state)
    if state.phase != 0 then throw new IllegalStateException("Ordinary business consumers end once after SQL settlement")
    ordinarySource=null;ordinaryPlan=null;ordinaryCommitted=null;ordinaryCompleted=null
    ordinaryDraft=null;ordinaryWitnessDraft=null;ordinarySettledNativeAcknowledgement=null
    current=null;proof=null;recallRecovery=None;recallHandoffs=Vector.empty;recallSqlHistory=Vector.empty
    commonDrafts.clear()
    state.phase=1

  private[mysql] def requireOriginalOrdinaryBorrowRetirement(budget: MySqlFormat3ReadBudget,
      state: MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement): Unit =
    requireOrdinaryExitState(state)
    if (root ne budget) || state.phase != 2 || native.retainedResponsibility then
      throw new IllegalStateException("actual Ordinary consumers/native must end before original borrow group")

  private[mysql] def beginOriginalOrdinaryBorrowRetirement(budget: MySqlFormat3ReadBudget,
      state: MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement): Unit =
    requireOriginalOrdinaryBorrowRetirement(budget,state)
    state.phase=3

  private[mysql] def requireOriginalTerminalTransfer(value: MySqlFull20NativeTransfer,
      state: MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement): Unit =
    requireOrdinaryExitState(state)
    if state.phase != 1 || (ordinaryTransfer ne value) then
      throw new IllegalStateException("exact original Ordinary transfer terminal identity required")

  private[mysql] def requireNativeExitPermit(permit: MySqlCertifiedOrdinaryNativeExitPermit,
      actual: MySqlCertifiedRestoreNativeHolder, actualRoot: MySqlFormat3ReadBudget): Unit =
    val state=result match
      case value: MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement => value
      case _ => throw new IllegalStateException("original Ordinary terminal state required")
    requireOrdinaryExitState(state)
    if (actual ne native) || (actualRoot ne root) || state.phase != 1 then
      throw new IllegalStateException("actual original Ordinary native consumer authority required")

  private[mysql] def requireTerminalAccounting(value: MySqlFull20ColdReadAccounting,
      state: MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement): Unit =
    requireOrdinaryExitState(state)
    var registered=false
    var index=0
    while index<accountings.size do
      if accountings(index) eq value then registered=true
      index+=1
    if state.phase < 1 || state.phase > 3 || !registered then
      throw new IllegalStateException("registered original terminal accounting required")

  private[mysql] def closeOrdinaryNativeAndBorrowGroup(state: MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement,
      permit: MySqlCertifiedOrdinaryNativeExitPermit): Unit =
    requireOrdinaryExitState(state)
    if state.phase != 1 then throw new IllegalStateException("Ordinary payload consumers must have ended")
    try
      native.closeOrdinaryForExit(permit,state)
      var pre=0
      while pre<accountings.size do
        accountings(pre).requireOriginalTerminalIdentity(this,state)
        pre+=1
      pre=0
      while pre<accountings.size do
        accountings(pre).endOrdinaryPayload(this,state)
        pre+=1
      ordinaryWitness=null;ordinaryTransfer=null;ordinaryInstalledScope=null;ordinarySettledRecord=null
      state.phase=2
      root.endCertifiedOrdinaryBorrowGroup(this,state)
      var i=0
      while i<accountings.size do
        accountings(i).detachOrdinaryTerminal(this,state)
        i+=1
      accountings.clear();parent=null;ordinaryWitness=null;ordinaryTransfer=null
      ordinaryInstalledScope=null;ordinarySettledRecord=null
      // 父root的实际close由Session按Boot/Actor exact identity去重；失败前不撤原根。
    catch
      case cause: Throwable =>
        state.retainFailure(cause)
        recordFailure(cause)
        throw cause

  private[mysql] def originalOrdinaryExitRoot(state: MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement): MySqlFormat3ReadBudget =
    requireOrdinaryExitState(state)
    state.originalRoot

  private[mysql] def ordinaryRootCloseReturned(state: MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement): Unit =
    requireOrdinaryExitState(state)
    if state.phase != 3 || !root.isClosed then throw new IllegalStateException("actual original Actor root close required")
    root=null;state.originalRoot=null;state.phase=4

  private[mysql] def settleForExit(): Unit = native.settleForExit()

  /** 原root/accounting/pending能力未真实结算时保留借用，不能清字段制造退休。 */
  private[mysql] def dropRuntimeGraphForExit(): Unit =
    require(Thread.currentThread() eq owner,"actual actor worker required")
    if runtimeGraphDropped then return
    require(root == null && accountings.isEmpty && commonDrafts.isEmpty && current == null &&
      parent == null && proof == null && ordinarySource == null && ordinaryPlan == null &&
      ordinaryDraft == null && ordinaryWitnessDraft == null && ordinaryWitness == null && ordinaryTransfer == null &&
      ordinaryCommitted == null && ordinaryCompleted == null && !native.retainedResponsibility,
      "certified actor still retains original root/accountings/pending/native responsibility")
    // 此处只撤销已退出actor的静态图入口；不关闭、丢弃或退款任何未结算原图。
    native.dropRuntimeConfigurationForExit()
    retainedExpectation=null;quotedSchema=null;verified=null;scope=null;held=null;qualification=null
    runtimeGraphDropped=true


  private final case class OrdinaryLockedImage(
      manifest: MySqlLockedManifest,
      binding: Option[PhysicalRecoveryBinding],
      writer: MySqlBusinessWriterRow,
      snapshot: MySqlFormat3FullSnapshot
  )

  /** Every ordinary SQL image is one actual locked RR transaction with checked rollback/idle restoration. */
  private def readOrdinaryLockedImage(): OrdinaryLockedImage =
    session.requireRecoveryActor(this)
    requireQualification()
    val rollback = new MySqlRollbackOnlyState
    rollbackOnly = rollback
    MySqlConnectionTransactionGate.enter(connection, rollback).foreach(throw _)
    var gate: MySqlConnectionTransactionGate.Owner = null
    var isolation = Option.empty[Int]
    var bodyFailure: Throwable = null
    var image: OrdinaryLockedImage = null
    try
      gate = MySqlConnectionTransactionGate.currentOwner(connection, rollback)
      isolation = Some(connection.getTransactionIsolation)
      require(connection.getAutoCommit, "ordinary locked read requires the exact idle auto-commit connection")
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      require(connection.getTransactionIsolation == Connection.TRANSACTION_REPEATABLE_READ,
        "ordinary locked read RR getter differs")
      connection.setAutoCommit(false)
      require(!connection.getAutoCommit, "ordinary locked read transaction getter differs")
      held.readmit(verified, expectation, scope)
      val manifest = lockManifest()
      val binding = lockBinding()
      val writer = readWriter(true)
      val reader = new MySqlCertifiedRecoveryInspectionActor.RecoveryLockedReadOwner(
        this, gate, rollback, manifest, binding, writer)
      val snapshot = new MySqlFormat3FullSnapshotReader(verified, scope, expectation.schema, root.profile, Some(ledger))
        .readWithinCertifiedRecoveryInspection(reader)
      image = OrdinaryLockedImage(manifest, binding, writer, snapshot)
      held.readmit(verified, expectation, scope)
    catch
      case cause: Throwable =>
        bodyFailure = cause
        rollback.mark(cause)
    finally
      val exactRollbackState = rollback
      val actions = new MySqlFreshPrepareCleanupActions:
        def rollback(): Unit = connection.rollback()
        def restoreAndObserveIdle(): Unit =
          connection.setAutoCommit(true)
          require(connection.getAutoCommit, "ordinary locked read autoCommit restore getter differs")
          isolation.foreach { previous =>
            connection.setTransactionIsolation(previous)
            require(connection.getTransactionIsolation == previous, "ordinary locked read isolation restore getter differs")
          }
          MySqlConnectionTransactionGate.leave(connection, exactRollbackState)
      MySqlFreshPrepareReadOnlyCleanup.finish(actions, Option(bodyFailure)) match
        case Left(cause) => bodyFailure = cause; rollback.mark(cause)
        case Right(_) => ()
    if bodyFailure != null then throw bodyFailure
    require(image != null, "actual locked full20 image was not retained")
    held.readmit(verified, expectation, scope)
    image

  private def executeOrdinaryWriterCas(plan: MySqlCertifiedOrdinaryCapabilities.PlannedCertifiedHandoff): Unit =
    session.requireRecoveryActor(this)
    require(plan != null && (ordinaryPlan eq plan), "exact registered ordinary W plan required")
    plan.source.requireFor(this, session, verified, scope, held)
    val rollback = new MySqlRollbackOnlyState
    rollbackOnly = rollback
    MySqlConnectionTransactionGate.enter(connection, rollback).foreach(throw _)
    var gate: MySqlConnectionTransactionGate.Owner = null
    var isolation = Option.empty[Int]
    var bodyFailure: Throwable = null
    try
      gate = MySqlConnectionTransactionGate.currentOwner(connection, rollback)
      isolation = Some(connection.getTransactionIsolation)
      require(connection.getAutoCommit, "ordinary W mutation requires the exact idle auto-commit connection")
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      require(connection.getTransactionIsolation == Connection.TRANSACTION_REPEATABLE_READ,
        "ordinary W mutation RR getter differs")
      connection.setAutoCommit(false)
      require(!connection.getAutoCommit, "ordinary W mutation transaction getter differs")
      held.readmit(verified, expectation, scope)
      val manifest = lockManifest()
      val binding = lockBinding()
      val current = readWriter(true)
      val before = readFullSnapshotInCurrentTransaction(gate, rollback, manifest, binding, current)
      MySqlOrdinaryHandoffGraph.requireOnlyWriterChanged(before, plan.target)
      require(before.relations == plan.before.relations && sameWriter(plan.beforeWriter, current) &&
        current.databaseNow.isBefore(plan.leaseDeadline),
        "second locked ordinary full20 image or exact source W row changed before the one CAS")
      plan.beginSqlCommit()
      val updated = updateOrdinaryWriter(plan.beforeWriter, plan.targetWriter)
      if updated != 1 then throw new SQLTransientConnectionException("ordinary W null-safe CAS changed no singleton row")
      // A returned commit is only the first acknowledgement; the next locked full20 readback still gates completion.
      connection.commit()
      plan.acknowledgedSqlCommit()
    catch
      case cause: Throwable =>
        bodyFailure = cause
        rollback.mark(cause)
    finally
      val exactRollbackState = rollback
      val actions = new MySqlFreshPrepareCleanupActions:
        def rollback(): Unit = connection.rollback()
        def restoreAndObserveIdle(): Unit =
          connection.setAutoCommit(true)
          require(connection.getAutoCommit, "ordinary W mutation autoCommit restore getter differs")
          isolation.foreach { previous =>
            connection.setTransactionIsolation(previous)
            require(connection.getTransactionIsolation == previous, "ordinary W mutation isolation restore getter differs")
          }
          MySqlConnectionTransactionGate.leave(connection, exactRollbackState)
      MySqlFreshPrepareReadOnlyCleanup.finish(actions, Option(bodyFailure)) match
        case Left(cause) => bodyFailure = cause; rollback.mark(cause)
        case Right(_) => ()
    if bodyFailure != null then throw bodyFailure
    held.readmit(verified, expectation, scope)

  private def readFullSnapshotInCurrentTransaction(gate: MySqlConnectionTransactionGate.Owner,
      rollback: MySqlRollbackOnlyState, manifest: MySqlLockedManifest,
      binding: Option[PhysicalRecoveryBinding], writer: MySqlBusinessWriterRow): MySqlFormat3FullSnapshot =
    val readOwner = new MySqlCertifiedRecoveryInspectionActor.RecoveryLockedReadOwner(
      this, gate, rollback, manifest, binding, writer)
    new MySqlFormat3FullSnapshotReader(verified, scope, expectation.schema, root.profile, Some(ledger))
      .readWithinCertifiedRecoveryInspection(readOwner)

  /** The sole ordinary continuation: retain original source DATA, install native HANDOFF, commit/read back W,
    * post-audit the same connection, settle native DB_ACQUIRED, then expose a private completion proof. */
  private[mysql] def initializeOrdinary(): MySqlCertifiedOrdinaryCapabilities.CompletedCertifiedHandoff =
    session.requireRecoveryActor(this)
    require(verified != null && scope != null && held != null && qualification != null && root != null &&
      !attempted && !ordinaryStarted && request.isInstanceOf[CertifiedSourceRequest.Ordinary],
      "one attached same-session ordinary certified source actor is required")
    attempted = true
    try
      session.beginCertifiedOrdinaryHandoff(this)
      ordinaryStarted = true
      requireQualification()
      if inputs.hasV2Origin then inputs.assemblyOwnerV2.withConfigurationWorkspace { native.beginOrdinary(inputs,parent) }
      else native.beginOrdinary(inputs,parent)
      requireQualification()

      val observation = native.coldObservation
      val oldCurrentV4 = native.selectedRecord
      var oldCurrentV5 = Option.empty[com.wi.floatingpod.server.B1ResumeRecordV5]
      var oldCurrentV5Bytes = Option.empty[Array[Byte]]
      var currentV5Identity = Option.empty[com.wi.floatingpod.server.WindowsMarkerIo.FileIdentity]
      val sourceRecord =
        if observation.status() == com.wi.floatingpod.server.WindowsB1ColdResumeLookup.Status.RESOLVED_DATA then
          val actual = native.captureSettledV5Source(parent)
          val record = actual.currentRecord()
          val bytes = actual.currentCanonicalBytes()
          val identity = actual.currentIdentity()
          require(record != null && bytes != null && bytes.nonEmpty && identity != null &&
            exactV5Data(record, bytes),
            "actual settled preinstall V5 current DATA, full bytes and native identity are required")
          val bridge = actual.v4Bridge()
          require(bridge != null && bridge.fileIdentity() == oldCurrentV4.fileIdentity() &&
            exactV4Bytes(bridge, oldCurrentV4),
            "actual settled V5 bridge must continue the exact selected V4 source identity and bytes")
          oldCurrentV5 = Some(record)
          oldCurrentV5Bytes = Some(bytes)
          currentV5Identity = Some(identity)
          record.transition()
        else if observation.status() == com.wi.floatingpod.server.WindowsB1ColdResumeLookup.Status.UNRESOLVED_DATA &&
            observation.unresolvedReason() == com.wi.floatingpod.server.WindowsB1ColdResumeLookup.UnresolvedReason.NO_V5 then
          require(native.historicalFull20Artifacts.nonEmpty,
            "actual complete NO_V5 discovery must retain every exact historical full20 artifact before ordinary planning")
          oldCurrentV4.record()
        else
          throw new java.io.IOException("ordinary certified restart requires an actual resolved settled V5 or complete NO_V5 V4 source")
      val sourceRecordBytes = com.wi.floatingpod.server.B1ResumeRecordCanonical.encode(sourceRecord)
      val source = new MySqlCertifiedRecoveryInspectionActor.VerifiedOriginalCertifiedSource(
        this, session, verified, scope, held, ordinaryRequest, oldCurrentV4, sourceRecord, sourceRecordBytes,
        oldCurrentV5, oldCurrentV5Bytes, currentV5Identity, root, native.previousNativeOwner,
        native.originalSourceIdentity, native.namespace)
      ordinarySource = source

      val before = readOrdinaryLockedImage()
      if before.writer.ownerBootUuid.nonEmpty && before.writer.leaseUntil.isAfter(before.writer.databaseNow) then
        throw new SQLTransientConnectionException("certified ordinary writer is active; no claim or replay")
      if MySqlNormalRecallRecovery.discover(before.snapshot, expectation.bindingUuid).nonEmpty then
        if oldCurrentV5.nonEmpty then native.captureRecallV5History(parent)
        // Complete native provenance still does not decide whether an old SQL C committed.
        require(compareWitnesses(), "original Recall recovery lacks complete verified native full20 history")
        if oldCurrentV5.nonEmpty then
          captureConfirmedRecallHandoffs()
          captureRecallSqlHistory(before.snapshot, before.writer)
      val currentIdentity = ordinaryRequest.currentIdentity
      val leaseDeadline = session.certifiedOrdinaryLeaseDeadline(this, before.writer.databaseNow)
      val receiptContinuation =
        if MySqlOrdinaryHandoffGraph.originalSlotMatches(before.writer, Option(sourceRecord.assertions().currentWriter().lastSlot())) then None
        else
          source.requireFor(this, session, verified, scope, held)
          native.requireSource()
          val data = MySqlNormalRecallRecovery.verifyLatestAnyReceiptData(before.snapshot, before.writer,
            source.previousOwner.boot(), sourceRecord.assertions().currentWriter().epoch(), root)
          root.reserveRetainedMemory(2048L, "original receipt approved scope comparison and holder")
          requireOriginalReceiptScope(data)
          Some(new MySqlCertifiedRecoveryInspectionActor.OriginalOrdinaryReceiptContinuation(
            this, source, before.snapshot, before.writer, data))
      ordinaryDraft = MySqlOrdinaryHandoffGraph.plan(before.snapshot, before.manifest, before.binding, before.writer,
        sourceRecord, native.originalSourceIdentity, inputs.bootId, inputs.bootGeneration, held.connectionGenerationId,
        held.actualConnectionId, currentIdentity.handoffAttempt(), currentIdentity.handoffOperation(), native.namespace,
        native.previousNativeOwner, native.targetNativeOwner, native.originalOwnerCanonicalBytes, leaseDeadline,
        root, expectation.format3Inputs.getOrElse(throw new IllegalStateException("ordinary handoff requires approved format3 audit inputs")),
        receiptContinuation)

      val draft = ordinaryDraft
      val planned = new MySqlCertifiedRecoveryInspectionActor.PlannedCertifiedHandoff(
        source, draft.transition.assertions().pendingAttempt(), draft.context, draft.beforeSnapshot, draft.targetSnapshot,
        draft.beforeWriter, draft.targetWriter, draft.beforeWriterAssertions, draft.targetWriterAssertions,
        draft.beforeGraph, draft.targetGraph, draft.beforeGraph.toResumeAssertions, draft.targetGraph.toResumeAssertions,
        draft.transition.assertions(), draft.transition, draft.canonicalBytes.clone(), draft.targetParity, draft.leaseDeadline)
      ordinaryPlan = planned
      source.registerPlan(planned)
      ordinaryWitnessDraft = MySqlFull20OriginalWitness.draft(planned, root)
      ordinaryWitness = MySqlFull20OriginalWitness.materialize(ordinaryWitnessDraft)
      ordinaryTransfer = ordinaryWitness.transferForNative()
      ordinaryTransfer.requireSameOriginalRoot(parent)
      planned.beginPublication(source)

      val nativeAcknowledgement =
        if oldCurrentV5.nonEmpty then
          native.beginExistingV5Handoff(parent, ordinaryTransfer)
          native.publishOrdinaryHandoffArtifact(ordinaryTransfer)
          native.retireOrdinaryFirstV5Slot()
          val durable = native.publishOrdinaryV5Handoff()
          MySqlCertifiedOrdinaryCapabilities.V5NativeHandoffAcknowledgement(durable)
        else
          native.beginExistingV4Handoff(ordinaryTransfer)
      planned.retainNativeAcknowledgement(nativeAcknowledgement)
      ordinaryInstalledScope = native.installOrdinaryHandoff()
      planned.retainInstalled()
      planned.requirePublishedAndInstalled()

      executeOrdinaryWriterCas(planned)
      val actual = readOrdinaryLockedImage()
      MySqlOrdinaryHandoffGraph.requireOnlyWriterChanged(planned.before, actual.snapshot)
      require(actual.snapshot.relations == planned.target.relations &&
        actual.snapshot.schema == planned.target.schema && actual.snapshot.connection == planned.target.connection &&
        actual.snapshot.observationScope == planned.target.observationScope &&
        actual.snapshot.maxAllowedPacketBytes == planned.target.maxAllowedPacketBytes &&
        sameWriter(planned.targetWriter, actual.writer) && planned.leaseDeadline.isAfter(actual.writer.databaseNow),
        "actual post-commit locked full20 image and W row must equal the exact planned target while its lease remains current")
      val postCommitObservedAtNanos = System.nanoTime()
      val commitEvidence = ordinaryCommitEvidence(planned, actual.writer)
      val unknownIdentity = planned.context.canonicalFingerprintSha256.toArray
      require(unknownIdentity.length == 32, "exact original ordinary operation fingerprint identity is required")
      val (token, receipt) = session.auditCommittedCertifiedHandoff(this, qualification, actual.writer)
      ordinaryCommitted = new MySqlCertifiedRecoveryInspectionActor.CommittedCertifiedHandoff(
        planned, actual.snapshot, actual.writer, oldCurrentV5, nativeAcknowledgement, postCommitObservedAtNanos,
        commitEvidence, unknownIdentity)

      val actualHandoff = nativeAcknowledgement match
        case value: MySqlCertifiedOrdinaryCapabilities.V4NativeHandoffAcknowledgement => value.value.record()
        case value: MySqlCertifiedOrdinaryCapabilities.V5NativeHandoffAcknowledgement => value.value.record().transition()
      val settled = MySqlOrdinaryHandoffGraph.settledTransition(ordinaryDraft, actualHandoff,
        ordinaryCommitted.commitEvidenceCopy, ordinaryCommitted.unknownDiagnosticIdentityCopy)
      ordinarySettledRecord = settled
      ordinarySettledNativeAcknowledgement = native.publishOrdinaryDbAcquired(ordinaryCommitted, settled)
      val facts = ordinaryFacts(oldCurrentV5, oldCurrentV5Bytes, currentV5Identity,
        nativeAcknowledgement, ordinarySettledNativeAcknowledgement, settled, actual.writer)
      val projection = new MySqlCertifiedRecoveryInspectionActor.MySqlOrdinaryPreAnvilProjection(
        this, source, ordinaryInstalledScope.installedWorld())
      ordinaryCompleted = new MySqlCertifiedRecoveryInspectionActor.CompletedCertifiedHandoff(
        ordinaryCommitted, this, verified, scope, token, receipt, facts, ordinarySettledNativeAcknowledgement, projection)
      ordinaryCommitted.markCompleted(ordinaryCompleted)
      val recalls = MySqlNormalRecallRecovery.discover(actual.snapshot, expectation.bindingUuid)
      if recalls.nonEmpty then
        require(recallSqlHistory.nonEmpty, "original C decision requires complete pre-handoff SQL history")
        val decisions = recalls.map(i => MySqlNormalRecallRecovery.classifyCheckpointData(
          planned.before, recallSqlHistory, i, root))
        root.reserveRetainedMemory(Math.addExact(512L, Math.multiplyExact(decisions.size.toLong, 256L)),
          "original restricted Recall admission and C decision holder")
        recallRecovery = Some(new MySqlCertifiedRecoveryInspectionActor.RecallRecoveryAdmission(
          this, ordinaryCompleted, ordinarySource, actual.snapshot, recalls, decisions))
      ordinaryCompleted
    catch
      case cause: Throwable =>
        try recordFailure(cause)
        catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, cleanup)
        throw cause

  /** Claim is private and one-shot; published immutable DATA facts cannot reconstruct this authority. */
  private[mysql] def claimCompletedCertifiedHandoff(
      completed: MySqlCertifiedOrdinaryCapabilities.CompletedCertifiedHandoff
  ): Unit =
    session.requireRecoveryActor(this)
    require((Thread.currentThread() eq owner) && !ordinaryClaimed && recallRecovery.isEmpty && completed != null &&
      (ordinaryCompleted eq completed) && ordinaryCommitted != null && ordinaryPlan != null && ordinarySource != null &&
      (completed.actor eq this) && (completed.committed eq ordinaryCommitted) &&
      (completed.verified eq verified) && (completed.scope eq scope) &&
      (completed.settledNativeAcknowledgement eq ordinarySettledNativeAcknowledgement) && primary.isEmpty,
      "the exact same-actor ordinary native/SQL completion capability may be claimed once")
    ordinaryPlan.requireCommitted()
    ordinarySource.requireFor(this, session, verified, scope, held)
    ordinaryCommitted.requireCompleted(completed)
    require(ordinarySettledRecord != null && ordinaryInstalledScope != null &&
      ordinarySettledNativeAcknowledgement != null && native.failure.isEmpty &&
      (completed.preAnvilProjection.actor eq this) && (completed.preAnvilProjection.source eq ordinarySource) &&
      (completed.preAnvilProjection.installedWorld eq ordinaryInstalledScope.installedWorld()),
      "exact actual installed world and durable settled native acknowledgement are required before session claim")
    native.requireOrdinaryInstalledContinuation(ordinaryInstalledScope,
      if nativeAcknowledgementIsV5(ordinaryPlan.actualNativeAcknowledgement) then native.ordinaryV5Flight else null)
    ordinaryClaimed = true

  /** 原Session分流只读取本actor实际保存的结果，不能caller提供空inventory以领取business。 */
  private[mysql] def originalRecallRecovery(completed: MySqlCertifiedOrdinaryCapabilities.CompletedCertifiedHandoff)
      : Option[MySqlCertifiedOrdinaryCapabilities.RecallRecoveryAdmission] =
    session.requireRecoveryActor(this)
    require((ordinaryCompleted eq completed) && !ordinaryClaimed && primary.isEmpty,
      "original unclaimed ordinary completion required")
    recallRecovery.foreach(_.requireCurrent())
    recallRecovery

  private def requireRecallRecovery(value: MySqlCertifiedRecoveryInspectionActor.RecallRecoveryAdmission): Unit =
    session.requireRecoveryActor(this)
    require((Thread.currentThread() eq owner) && recallRecovery.exists(_ eq value) && !ordinaryClaimed &&
      primary.isEmpty && (value.completed eq ordinaryCompleted) && (value.source eq ordinarySource) &&
      value.inventory.nonEmpty && !value.snapshot.allocationBudget.isClosed,
      "original live restricted Recall actor/holder required")
    ordinarySource.requireFor(this, session, verified, scope, held)
    ordinaryPlan.requireCommitted()
    ordinaryCommitted.requireCompleted(ordinaryCompleted)
    native.requireOrdinaryInstalledContinuation(ordinaryInstalledScope,
      if nativeAcknowledgementIsV5(ordinaryPlan.actualNativeAcknowledgement) then native.ordinaryV5Flight else null)

  /** 同一恢复holder只接收原coordinator真正续约并post-audit后的窗口，不重建source资格。 */
  private[mysql] def maintainRecallRecovery(value: MySqlCertifiedOrdinaryCapabilities.RecallRecoveryAdmission,
      coordinator: MySqlWriterFenceCoordinator): Unit =
    requireRecallRecovery(value)
    session.requireCertifiedOrdinaryCoordinator(this, coordinator)
    try
      val (token, receipt) = coordinator.renewRecallRecovery(value)
      qualification = token.qualification
      value.currentToken = token
      value.currentReceipt = receipt
      requireRecallRecovery(value)
    catch
      case cause: Throwable =>
        recordFailure(cause)
        throw cause

  /** Called by the coordinator before business admission, after the Session consumed the exact actor claim. */
  private[mysql] def validateCompletedHandoff(
      completed: MySqlCertifiedOrdinaryCapabilities.CompletedCertifiedHandoff,
      expected: MySqlSchemaAuditExpectation,
      coordinator: MySqlWriterFenceCoordinator
  ): Unit =
    session.requireRecoveryActor(this)
    session.requireCertifiedOrdinaryCoordinator(this, coordinator)
    require((Thread.currentThread() eq owner) && ordinaryClaimed && completed != null &&
      (ordinaryCompleted eq completed) && expected == expectation &&
      ordinarySource != null && ordinaryPlan != null && ordinaryCommitted != null &&
      (completed.actor eq this) && (completed.committed eq ordinaryCommitted) &&
      (completed.verified eq verified) && (completed.scope eq scope) && primary.isEmpty &&
      ordinaryPlan.retainedFailure.isEmpty && ordinaryCommitted.retainedFailure.isEmpty && native.failure.isEmpty,
      "exact actor/session/qualification/native responsibilities must remain current through coordinator admission")
    requireQualification()
    ordinarySource.requireFor(this, session, verified, scope, held)
    ordinaryPlan.requireCommitted()
    ordinaryCommitted.requireCompleted(completed)
    native.requireOrdinaryInstalledContinuation(ordinaryInstalledScope,
      if nativeAcknowledgementIsV5(ordinaryPlan.actualNativeAcknowledgement) then native.ordinaryV5Flight else null)
    require(ordinarySettledRecord != null && ordinarySettledNativeAcknowledgement != null &&
      (completed.settledNativeAcknowledgement eq ordinarySettledNativeAcknowledgement) &&
      (completed.preAnvilProjection.installedWorld eq ordinaryInstalledScope.installedWorld()),
      "the exact cold-compared final DB_ACQUIRED source custody must remain installed before admission")
    ordinarySettledNativeAcknowledgement match
      case value: MySqlCertifiedOrdinaryCapabilities.V5SettledNativeAcknowledgement =>
        val actual = value.value.record()
        require(actual != null && Arrays.equals(com.wi.floatingpod.server.B1ResumeRecordCanonical.encode(actual.transition()),
          com.wi.floatingpod.server.B1ResumeRecordCanonical.encode(ordinarySettledRecord)),
          "actual final V5 DB_ACQUIRED native ACK differs from actor-settled transition")
      case _: MySqlCertifiedOrdinaryCapabilities.V4SettledNativeAcknowledgement =>
        throw new IllegalStateException("ordinary business admission requires the completed V5 DB_ACQUIRED bridge")

  private def nativeAcknowledgementIsV5(
      value: MySqlCertifiedOrdinaryCapabilities.OrdinaryNativeHandoffAcknowledgement
  ): Boolean = value match
    case _: MySqlCertifiedOrdinaryCapabilities.V5NativeHandoffAcknowledgement => true
    case _: MySqlCertifiedOrdinaryCapabilities.V4NativeHandoffAcknowledgement => false

  private def ordinaryCommitEvidence(
      plan: MySqlCertifiedOrdinaryCapabilities.PlannedCertifiedHandoff,
      actual: MySqlBusinessWriterRow
  ): Array[Byte] =
    require(plan != null && actual != null && held != null && ordinaryDraft != null,
      "same-actor exact ordinary W plan and actual post-commit row required")
    require(plan.attempt.connectionId() == held.actualConnectionId &&
      plan.attempt.connectionGeneration() == held.connectionGenerationId &&
      plan.attempt.context().boot() == inputs.bootId && plan.attempt.bootGeneration() == inputs.bootGeneration &&
      plan.attempt.namespace() == ordinarySource.namespace &&
      Arrays.equals(plan.attempt.context().originalOperationFingerprint(), plan.context.canonicalFingerprintSha256.toArray),
      "actual original commit evidence session/context differs from its planned attempt")
    MySqlOrdinaryHandoffGraph.commitEvidence(plan.attempt, plan.beforeWriter, plan.targetWriter, actual, root)
  private def ordinaryFacts(
      originalV5: Option[com.wi.floatingpod.server.B1ResumeRecordV5],
      originalV5Bytes: Option[Array[Byte]],
      originalV5Identity: Option[com.wi.floatingpod.server.WindowsMarkerIo.FileIdentity],
      handoff: MySqlCertifiedOrdinaryCapabilities.OrdinaryNativeHandoffAcknowledgement,
      settledAck: MySqlCertifiedOrdinaryCapabilities.OrdinaryNativeSettledAcknowledgement,
      settled: com.wi.floatingpod.server.B1ResumeRecord,
      writer: MySqlBusinessWriterRow
  ): MySqlOrdinaryFacts =
    require(originalV5.isDefined == originalV5Bytes.isDefined && originalV5.isDefined == originalV5Identity.isDefined &&
      handoff != null && settledAck != null && settled != null && writer != null,
      "actual optional V5 source and exact native handoff/settled DATA are required")
    val records = ArrayBuffer.empty[MySqlOrdinaryV5RecordFact]
    originalV5.foreach { record =>
      val bytes = originalV5Bytes.get
      val identity = originalV5Identity.get
      records += ordinaryV5Fact(record, bytes, identity)
    }
    handoff match
      case value: MySqlCertifiedOrdinaryCapabilities.V5NativeHandoffAcknowledgement =>
        val record = value.value.record()
        root.reserveTemporaryMemory(65_536L, "exact canonical V5 HANDOFF DATA facts before allocation")
        var bytes: Array[Byte] = null
        var primaryFailure: Throwable = null
        try
          bytes = com.wi.floatingpod.server.B1ResumeRecordV5.encodeData(record)
          records += ordinaryV5Fact(record, bytes, value.value.identity())
        catch
          case cause: Throwable =>
            primaryFailure = cause
            throw cause
        finally
          if bytes != null then Arrays.fill(bytes, 0.toByte)
          try root.releaseTemporaryMemory(65_536L, "canonical V5 HANDOFF fact bytes dropped after exact ACK fact capture")
          catch
            case cleanup: Throwable =>
              if primaryFailure != null then MySqlOwnedSessionLifecycle.suppress(primaryFailure, cleanup)
              else throw cleanup
      case _: MySqlCertifiedOrdinaryCapabilities.V4NativeHandoffAcknowledgement => ()
    settledAck match
      case value: MySqlCertifiedOrdinaryCapabilities.V5SettledNativeAcknowledgement =>
        val record = value.value.record()
        require(Arrays.equals(com.wi.floatingpod.server.B1ResumeRecordCanonical.encode(record.transition()),
          com.wi.floatingpod.server.B1ResumeRecordCanonical.encode(settled)),
          "actual settled V5 native record must equal the exact actor DB_ACQUIRED transition")
        val bytes = value.value.canonicalBytes()
        try records += ordinaryV5Fact(record, bytes, value.value.identity())
        finally Arrays.fill(bytes, 0.toByte)
      case _: MySqlCertifiedOrdinaryCapabilities.V4SettledNativeAcknowledgement =>
        throw new IllegalStateException("ordinary completion requires the actual settled V5 native bridge")
    require(records.nonEmpty && records.map(_.sequence).sliding(2).forall(pair => pair.size < 2 || pair(0) < pair(1)),
      "actual ordinary V5 source/handoff/settled records must retain strictly ordered sequence facts")
    MySqlOrdinaryFacts(ordinaryRequest.approvedOriginalCreation, inputs.bootId, inputs.bootGeneration,
      scope.connectionGeneration, writer.writerEpoch, ordinarySource.namespace, native.originalSourceIdentity,
      native.originalGuardFileIdentity, native.originalReceiptFileIdentity, native.originalAnchorFileIdentity,
      native.originalReceiptSha256, native.originalAnchorSha256, records.toVector)

  private def ordinaryV5Fact(
      record: com.wi.floatingpod.server.B1ResumeRecordV5,
      canonical: Array[Byte],
      identity: com.wi.floatingpod.server.WindowsMarkerIo.FileIdentity
  ): MySqlOrdinaryV5RecordFact =
    require(record != null && canonical != null && canonical.nonEmpty && canonical.length <= 65_536 && identity != null,
      "complete exact actual V5 record bytes and file identity are required")
    root.reserveTemporaryMemory(65_536L, "canonical V5 record identity comparison before reference facts")
    var encoded: Array[Byte] = null
    var primaryFailure: Throwable = null
    try
      encoded = com.wi.floatingpod.server.B1ResumeRecordV5.encodeData(record)
      require(Arrays.equals(encoded, canonical), "complete actual V5 record bytes differ from its native DATA")
      val artifacts = record.allReferences()
      root.reserveRetainedMemory(Math.addExact(512L, Math.multiplyExact(artifacts.size().toLong, 256L)),
        "actual V5 referenced full20 native identities retained before fact allocation")
      val references = artifacts.asScala.toVector.filter(_ != null).map { reference =>
        require(reference.fileIdentity() != null, "actual complete V5 artifact reference identity required")
        reference.fileIdentity().versionedIdentity()
      }
      val digest = com.wi.floatingpod.server.WindowsWorldProvisioner.digest(canonical)
      MySqlOrdinaryV5RecordFact(record.transition().sequence(), java.util.HexFormat.of().withUpperCase().formatHex(digest),
        identity.versionedIdentity(), references)
    catch
      case cause: Throwable =>
        primaryFailure = cause
        throw cause
    finally
      if encoded != null then Arrays.fill(encoded, 0.toByte)
      try root.releaseTemporaryMemory(65_536L, "V5 record comparison bytes dropped after complete fact capture")
      catch
        case cleanup: Throwable =>
          if primaryFailure != null then MySqlOwnedSessionLifecycle.suppress(primaryFailure, cleanup)
          else throw cleanup

  private def exactV5Data(
      record: com.wi.floatingpod.server.B1ResumeRecordV5,
      actual: Array[Byte]
  ): Boolean =
    require(record != null && actual != null && actual.nonEmpty && actual.length <= 65_536,
      "complete bounded actual V5 current DATA is required")
    root.reserveTemporaryMemory(65_536L, "exact actual V5 source DATA comparison before ordinary plan")
    var expected: Array[Byte] = null
    var primaryFailure: Throwable = null
    try
      expected = com.wi.floatingpod.server.B1ResumeRecordV5.encodeData(record)
      Arrays.equals(expected, actual)
    catch
      case cause: Throwable =>
        primaryFailure = cause
        throw cause
    finally
      if expected != null then Arrays.fill(expected, 0.toByte)
      try root.releaseTemporaryMemory(65_536L, "exact V5 source comparison bytes dropped")
      catch
        case cleanup: Throwable =>
          if primaryFailure != null then MySqlOwnedSessionLifecycle.suppress(primaryFailure, cleanup)
          else throw cleanup

  private def exactV4Bytes(
      leftFacts: com.wi.floatingpod.server.WindowsB1TwoSlotStore.ResumeStoredRecordFacts,
      rightFacts: com.wi.floatingpod.server.WindowsB1TwoSlotStore.ResumeStoredRecordFacts
  ): Boolean =
    require(leftFacts != null && rightFacts != null, "complete actual V4 bridge facts are required")
    root.reserveTemporaryMemory(2L * 65_536L, "exact V4 source/bridge byte comparison before ordinary plan")
    var left: Array[Byte] = null
    var right: Array[Byte] = null
    var primaryFailure: Throwable = null
    try
      left = leftFacts.canonicalBytes()
      right = rightFacts.canonicalBytes()
      require(left.nonEmpty && right.nonEmpty && left.length <= 65_536 && right.length <= 65_536,
        "complete bounded actual V4 bridge bytes are required")
      Arrays.equals(left, right)
    catch
      case cause: Throwable =>
        primaryFailure = cause
        throw cause
    finally
      if left != null then Arrays.fill(left, 0.toByte)
      if right != null then Arrays.fill(right, 0.toByte)
      try root.releaseTemporaryMemory(2L * 65_536L, "V4 source/bridge comparison bytes dropped")
      catch
        case cleanup: Throwable =>
          if primaryFailure != null then MySqlOwnedSessionLifecycle.suppress(primaryFailure, cleanup)
          else throw cleanup

  private def sameWriter(expected: MySqlBusinessWriterRow, actual: MySqlBusinessWriterRow): Boolean =
    expected != null && actual != null && expected.copy(databaseNow = actual.databaseNow) == actual

  /** Only the actual consumed native chain supplies edges; SQL receipt segments are still required separately. */
  private def captureConfirmedRecallHandoffs(): Unit =
    ordinarySource.requireFor(this, session, verified, scope, held)

    require(recallHandoffs.isEmpty, "original Recall handoff chain is captured once")
    val records = native.consumedRecallV5History(parent)
    require(records.size() > 0 && records.size() % 2 == 1, "complete settled ordinary V5 history required")
    root.reserveRetainedMemory(Math.addExact(4096L, Math.multiplyExact(accountings.size.toLong + records.size().toLong, 512L)),
      "original Recall confirmed edge and matched witness indexes")
    val result = Vector.newBuilder[RecallConfirmedHandoff]
    var index = 0
    while index < records.size() do
      val settled = records.get(index).transition()
      val handoff = if index + 1 < records.size() then records.get(index + 1).transition()
        else native.selectedRecord.record()
      val data = MySqlOrdinaryHandoffGraph.requireConfirmedHandoffData(handoff, settled, root)
      val attempt = handoff.assertions().pendingAttempt()
      val matching = accountings.filter(a => a.witness != null && a.matchedWitnessAttempt().attemptId() == attempt.attemptId())
      require(matching.nonEmpty, "confirmed handoff lacks its original full20 artifact")
      matching.foreach { a =>
        root.reserveRetainedMemory(1048576L, "complete original handoff artifact record and attempt comparison")
        require(Arrays.equals(B1ResumeRecordCanonical.encodeAttemptData(a.matchedWitnessAttempt()),
          B1ResumeRecordCanonical.encodeAttemptData(attempt)) &&
          Arrays.equals(B1ResumeRecordCanonical.encode(a.witness.originalRecord), B1ResumeRecordCanonical.encode(handoff)),
          "confirmed handoff differs from exact original artifact record/attempt")
        require(a.witness.before.snapshot.relations == matching.head.witness.before.snapshot.relations &&
          a.witness.target.snapshot.relations == matching.head.witness.target.snapshot.relations,
          "same original handoff has conflicting full20 images")
      }
      val witness = matching.head.witness
      result += RecallConfirmedHandoff(data, witness.before.snapshot, witness.target.snapshot)
      index += 2
    recallHandoffs = result.result()
    ordinarySource.requireFor(this, session, verified, scope, held)

  private def requireOriginalReceiptScope(data: MySqlNormalRecallRecovery.ReceiptData): Unit =
    val historical = data.scope.stableSession
    val format3 = expectation.format3Inputs.getOrElse(throw new IllegalStateException("original receipt requires approved format3 inputs"))
    require(historical.provisioning == expectation.provisioningUuid && historical.ddl.equalsIgnoreCase(expectation.ddlSha256) &&
      historical.manifest.equalsIgnoreCase(expectation.objects.canonicalSha256) &&
      historical.configuration.equalsBytes(format3.configurationSha256.bytes) &&
      historical.provenanceKind == format3.sourceProvenanceKind &&
      historical.provenance.equalsBytes(format3.sourceProvenanceSha256.bytes) && historical.namespace == held.namespace.lockName(),
      "original receipt scope differs from actual approved source/configuration/barrier")
    data.recall.foreach { recall =>
      require(recall.phase.header.source.sourceIdentity == native.originalSourceIdentity &&
        recall.phase.header.source.worldIdentity == native.originalSourceIdentity,
        "original Recall source differs from actually held native world")
    }

  /** Complete SQL history stays private until the fresh actual fencing handoff also succeeds. */
  private def captureRecallSqlHistory(inventory: MySqlFormat3FullSnapshot, actualWriter: MySqlBusinessWriterRow): Unit =
    ordinarySource.requireFor(this, session, verified, scope, held)
    require(recallHandoffs.nonEmpty && recallSqlHistory.isEmpty, "original confirmed Recall edges required once")
    val count = inventory.relation("fp_recovery_operation").rows.size.toLong
    require(Math.addExact(count, recallHandoffs.size.toLong) <= root.profile.maxMembers(), "complete Recall history member limit")
    root.reserveRetainedMemory(Math.addExact(8192L, Math.multiplyExact(count + recallHandoffs.size.toLong, 512L)),
      "complete Recall SQL segments, unique receipts and original anchor index")
    val seen = scala.collection.mutable.HashSet.empty[(UUID, UUID)]
    val segments = Vector.newBuilder[MySqlNormalRecallRecovery.ReceiptHistoryData]
    var cursor = inventory
    var stopped = actualWriter
    def segment(anchor: MySqlFormat3FullSnapshot): Unit =
      val result = MySqlNormalRecallRecovery.verifyStoppedReceiptHistoryData(inventory, cursor, anchor, stopped, root)
      result.newestFirst.foreach { resolved =>
        require(seen.add((resolved.context.bindingUuid, resolved.context.operationUuid)) && seen.size.toLong <= count,
          "duplicate or excessive original receipt across epoch segments")
        root.reserveRetainedMemory(2048L, "original historical approved scope defensive comparison")
        requireOriginalReceiptScope(resolved)
      }
      segments += result
    recallHandoffs.zipWithIndex.foreach { (edge, index) =>
      if index + 1 < recallHandoffs.size then
        require(edge.data.handoff.assertions().previousOwner() == recallHandoffs(index + 1).data.settled.assertions().currentOwner(),
          "original native owners do not join across confirmed epoch edges")
      segment(edge.target)
      cursor = edge.before
      stopped = edge.data.before
    }
    val certify = native.selectedRecord.record().assertions().acknowledgedCertifyWitness()
    require(certify != null && certify.phase() == B1ResumeRecord.Phase.CERTIFY, "original CERTIFY anchor required")
    val matching = accountings.filter(a => a.witness != null && a.matchedWitnessAttempt().attemptId() == certify.attemptId())
    require(matching.nonEmpty, "original complete CERTIFY artifact missing")
    val previous = recallHandoffs.last.data.handoff.assertions().previousOwner()
    matching.foreach { a =>
      root.reserveRetainedMemory(1048576L, "complete original CERTIFY attempt and duplicate artifact comparison")
      require(Arrays.equals(B1ResumeRecordCanonical.encodeAttemptData(a.matchedWitnessAttempt()), B1ResumeRecordCanonical.encodeAttemptData(certify)) &&
        a.witness.originalRecord.assertions().currentOwner() == previous &&
        certify.context().boot() == previous.boot() && certify.bootGeneration() == previous.generation() &&
        a.witness.envelope == matching.head.witness.envelope,
        "original CERTIFY anchor differs from first handoff predecessor owner/attempt/full artifact")
    }
    segment(matching.head.witness.target.snapshot)
    ordinarySource.requireFor(this, session, verified, scope, held)
    recallSqlHistory = segments.result()
  private def updateOrdinaryWriter(before: MySqlBusinessWriterRow, target: MySqlBusinessWriterRow): Int =
    val sql = "UPDATE " + table("fp_runtime_writer") +
      " SET writer_epoch=?,owner_boot_uuid=?,lease_until=? WHERE singleton_fixed1=1 AND binding_uuid=? " +
      "AND writer_epoch=? AND owner_boot_uuid <=> ? AND lease_until=? " +
      "AND last_commit_operation_uuid <=> ? AND last_commit_fingerprint <=> ? " +
      "AND (owner_boot_uuid IS NULL OR lease_until<=UTC_TIMESTAMP(6))"
    withStatement(sql) { statement =>
      statement.setLong(1, target.writerEpoch)
      statement.setBytes(2, uuidBytes(target.ownerBootUuid.getOrElse(
        throw new IllegalStateException("ordinary target owner boot is required"))))
      statement.setObject(3, target.leaseUntil)
      statement.setBytes(4, uuidBytes(before.bindingUuid))
      statement.setLong(5, before.writerEpoch)
      before.ownerBootUuid match
        case Some(value) => statement.setBytes(6, uuidBytes(value))
        case None => statement.setBytes(6, null)
      statement.setObject(7, before.leaseUntil)
      before.lastOperationUuid match
        case Some(value) => statement.setBytes(8, uuidBytes(value))
        case None => statement.setBytes(8, null)
      before.lastFingerprint match
        case Some(value) => statement.setBytes(9, value.toArray)
        case None => statement.setBytes(9, null)
      statement.executeUpdate()
    }

  private def uuidBytes(value: UUID): Array[Byte] =
    require(value != null, "exact actual writer UUID required")
    java.nio.ByteBuffer.allocate(16).putLong(value.getMostSignificantBits).putLong(value.getLeastSignificantBits).array()

  private[mysql] def inspect(): MySqlCertifiedInspectionObservation =
    session.requireRecoveryActor(this)
    require(verified != null && !attempted, "registered attached inspection is one-shot")
    attempted = true
    try
      requireQualification()
      native.begin(inputs, parent)
      requireQualification()
      val observed = native.coldObservation
      if observed.status() != com.wi.floatingpod.server.WindowsB1ColdResumeLookup.Status.RESOLVED_DATA then
        result = observation("UNRESOLVED_NATIVE_DATA")
      else
        val rollback = new MySqlRollbackOnlyState
        rollbackOnly = rollback // retained before entering actual transaction gate
        MySqlConnectionTransactionGate.enter(connection, rollback).foreach(throw _)
        var gate: MySqlConnectionTransactionGate.Owner = null
        var bodyFailure: Throwable = null
        var matched = false
        var isolation = Option.empty[Int]
        try
          gate = MySqlConnectionTransactionGate.currentOwner(connection, rollback)
          isolation = Some(connection.getTransactionIsolation)
          require(connection.getAutoCommit, "inspection requires idle actual connection")
          connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
          require(connection.getTransactionIsolation == Connection.TRANSACTION_REPEATABLE_READ, "inspection RR getter differs")
          connection.setAutoCommit(false)
          require(!connection.getAutoCommit, "inspection transaction getter differs")
          held.readmit(verified, expectation, scope)
          val manifest = lockManifest()
          val binding = lockBinding()
          val writer = readWriter(true)
          if writer.ownerBootUuid.nonEmpty && writer.leaseUntil.isAfter(writer.databaseNow) then
            throw new SQLTransientConnectionException("certified inspection writer is active; no claim or replay")
          val readOwner = new MySqlCertifiedRecoveryInspectionActor.RecoveryLockedReadOwner(this, gate, rollback, manifest, binding, writer)
          current = new MySqlFormat3FullSnapshotReader(verified, scope, expectation.schema, root.profile, Some(ledger))
            .readWithinCertifiedRecoveryInspection(readOwner)
          matched = compareWitnesses()
          held.readmit(verified, expectation, scope)
        catch
          case cause: Throwable =>
            bodyFailure = cause
            rollback.mark(cause)
            recordFailure(cause)
        finally
          // false->true may commit an uncertain transaction. Rollback failure must
          // retain this original gate/transaction; Session then checked-closes it.
          val exactRollbackState = rollback
          val actions = new MySqlFreshPrepareCleanupActions:
            def rollback(): Unit = connection.rollback()
            def restoreAndObserveIdle(): Unit =
              connection.setAutoCommit(true)
              require(connection.getAutoCommit, "inspection autoCommit restore getter differs")
              isolation.foreach { previous =>
                connection.setTransactionIsolation(previous)
                require(connection.getTransactionIsolation == previous, "inspection isolation restore getter differs")
              }
              MySqlConnectionTransactionGate.leave(connection, exactRollbackState)
          MySqlFreshPrepareReadOnlyCleanup.finish(actions, Option(bodyFailure)) match
            case Left(cause) =>
              bodyFailure = cause
              rollback.mark(cause)
              recordFailure(cause)
            case Right(_) => ()
        if bodyFailure != null then throw bodyFailure
        if matched then
          root.reserveRetainedMemory(512L, "historical DATA comparison proof after actual rollback/restoration/gate leave")
          proof = new ColdOriginalComparisonProof(current, held.actualConnectionId)
          result = observation("HISTORICAL_COMPARISON_CONFIRMED_NOT_ADMITTED")
        else result = observation("UNPROVEN_ORIGINAL_EFFECT_DOMAIN")
      result match
        case value: MySqlCertifiedInspectionObservation => value
        case _ => throw new IllegalStateException("inspection observation phase required")
    catch
      case cause: Throwable =>
        recordFailure(cause)
        throw cause

  private def observation(status: String): MySqlCertifiedInspectionObservation =
    root.reserveRetainedMemory(1024L, "at most six immutable witness length observations before allocation")
    val lengths = accountings.iterator.filter(_.witness != null).map { a =>
      MySqlCertifiedWitnessLengths(a.witness.contextCanonical.length, a.witness.before.byteLength,
        a.witness.target.byteLength, a.witness.envelope.length)
    }.toVector
    MySqlCertifiedInspectionObservation(status, Some(native.originalSourceIdentity), Some(native.originalReceiptSha256),
      Some(native.selectedRecord.record().sequence()), native.retainedResponsibility, root != null, primary, lengths)

  /** Private historical comparison only: old lock inactive; future writer plan must requalify. */
  private final class ColdOriginalComparisonProof(val fullCurrent: MySqlFormat3FullSnapshot, val observedSuccessorCid: Long)

  private def compareWitnesses(): Boolean =
    var complete = true
    var witnesses = 0
    var selectedWitnesses = 0
    val observedCurrent = native.coldObservation.currentRecord()
    val selectedState = if observedCurrent != null then observedCurrent.transition().assertions()
      else native.selectedRecord.record().assertions()
    val pending = selectedState.pendingAttempt()
    val latestSettlement = selectedState.latestSettlement()
    val currentSlot = selectedState.currentWriter().lastSlot()
    accountings.foreach { a =>
      if a.witness != null then
        witnesses += 1
        val w = a.witness
        val attempt = a.matchedWitnessAttempt()
        require(attempt != null && attempt.namespace() == held.namespace.lockName(), "historical original namespace differs from held successor")
        root.reserveRetainedMemory(3L * 65536L, "complete common assertions defensive access before decode")
        val before = MySqlB1CommonGraphColdCodec.draft(attempt.beforeGraph(), w.before.snapshot, root)
        commonDrafts += before
        val beforeGraph = MySqlB1CommonGraphColdCodec.decode(before, expectation.format3Inputs.get)
        val target = MySqlB1CommonGraphColdCodec.draft(attempt.targetGraph(), w.target.snapshot, root)
        commonDrafts += target
        val targetGraph = MySqlB1CommonGraphColdCodec.decode(target, expectation.format3Inputs.get)
        MySqlFormat3B1ContextCorrespondence.requireCorrespondence(beforeGraph, w.before.snapshot, expectation.format3Inputs.get, root)
        MySqlFormat3B1ContextCorrespondence.requireCorrespondence(targetGraph, w.target.snapshot, expectation.format3Inputs.get, root)
        require(w.before.snapshot.observationScope.bootUuid == attempt.context().boot() &&
          w.before.snapshot.observationScope.bootGeneration == attempt.bootGeneration() &&
          w.before.snapshot.schema == expectation.schema && w.before.snapshot.connection.serverUuid == expectation.serverUuid,
          "historical header differs from complete original context")
        require(w.target.snapshot.observationScope == w.before.snapshot.observationScope &&
          w.target.snapshot.connection == w.before.snapshot.connection &&
          w.target.snapshot.schema == w.before.snapshot.schema &&
          w.target.snapshot.maxAllowedPacketBytes == w.before.snapshot.maxAllowedPacketBytes,
          "historical target image header differs from original before image")
        require(attempt.context().binding() == attempt.targetWriter().binding() &&
          attempt.context().writerEpoch() == attempt.targetWriter().epoch(),
          "original operation context differs from target writer")
        val isHandoff = attempt.phase() == com.wi.floatingpod.server.B1ResumeRecord.Phase.HANDOFF
        requireHistoricalWriter(w.before.snapshot, attempt.beforeWriter(), isHandoff)
        requireHistoricalWriter(w.target.snapshot, attempt.targetWriter(), isHandoff)
        if isHandoff then
          MySqlOrdinaryHandoffGraph.requireOnlyWriterChanged(w.before.snapshot, w.target.snapshot)
          if request.isInstanceOf[CertifiedSourceRequest.Ordinary] then
            MySqlCertifiedRecoveryInspectionActor.requireRecallHistoricalWriterStep(attempt, w.originalRecord)
          require(exactReceiptSlot(attempt.beforeWriter().lastSlot(), attempt.targetWriter().lastSlot()) &&
            Option(attempt.beforeWriter().lastSlot()).forall(slot =>
              slot.binding() == attempt.beforeWriter().binding() && slot.writerEpoch() <= attempt.beforeWriter().epoch()),
            "historical ordinary HANDOFF must preserve its original slot and cannot mint a new receipt")
        if !requireFreshEffect(attempt, beforeGraph, targetGraph, w.before.snapshot, w.target.snapshot) then complete = false
        // Original prepare/older settled artifacts remain fully checked, but are not
        // falsely required to be the current target after a later actual transition.
        val selected = if pending != null then attempt.attemptId() == pending.attemptId()
          else if latestSettlement != null then attempt.attemptId() == latestSettlement.settledAttempt().attemptId()
          else currentSlot != null && attempt.logicalWriteUuid() == currentSlot.operation()
        if selected then
          selectedWitnesses += 1
          if request.isInstanceOf[CertifiedSourceRequest.Inspection] && current.relations != w.target.snapshot.relations then complete = false
    }
    require(witnesses > 0, "resolved lookup lacks a complete retained original witness")
    complete && selectedWitnesses > 0

  private def exactReceiptSlot(left: com.wi.floatingpod.server.B1ResumeRecord.ReceiptSlotAssertions,
      right: com.wi.floatingpod.server.B1ResumeRecord.ReceiptSlotAssertions): Boolean =
    (Option(left), Option(right)) match
      case (None, None) => true
      case (Some(a), Some(b)) => a.boot() == b.boot() && a.writerEpoch() == b.writerEpoch() &&
        a.binding() == b.binding() && a.operation() == b.operation() && Arrays.equals(a.fingerprint(), b.fingerprint())
      case _ => false

  private def requireHistoricalWriter(snapshot: MySqlFormat3FullSnapshot,
      assertions: com.wi.floatingpod.server.B1ResumeRecord.WriterAssertions, handoff: Boolean = false): Unit =
    val slot = Option(assertions.lastSlot())
    slot.foreach { value =>
      require(value.binding() == assertions.binding() &&
        (if handoff then value.writerEpoch() <= assertions.epoch() else value.writerEpoch() == assertions.epoch()),
        "historical receipt slot binding/epoch differs from its writer")
    }
    root.reserveRetainedMemory(1024L, "historical writer assertion wrappers and defensive fingerprint before allocation")
    MySqlFreshFormat3Graph.requireWriterMatches(snapshot, MySqlBusinessWriterRow(assertions.binding(),
      assertions.epoch(), Option(assertions.ownerBoot()), assertions.leaseUntil(), slot.map(_.operation()),
      slot.map(_.fingerprint().toVector), assertions.leaseUntil()))

  private def requireFreshEffect(attempt: com.wi.floatingpod.server.B1ResumeRecord.AttemptAssertions,
      before: MySqlB1CommonGraph, target: MySqlB1CommonGraph, fullBefore: MySqlFormat3FullSnapshot, fullTarget: MySqlFormat3FullSnapshot): Boolean =
    val effects = attempt.context().effects()
    val handoff = attempt.phase() == com.wi.floatingpod.server.B1ResumeRecord.Phase.HANDOFF
    val expectedTarget = if handoff then "certified-ordinary/writer" else "bootstrap/full20-graph"
    if effects.size() != 1 || effects.get(0).targetIdentity() != expectedTarget then return false
    if handoff then MySqlOrdinaryHandoffGraph.requireOnlyWriterChanged(fullBefore, fullTarget)
    else MySqlCertifiedRecoveryInspectionActor.requireFreshTargetRelations(fullBefore, fullTarget, root)
    val p = request match
      case CertifiedSourceRequest.Inspection(value) => value.plan
      case CertifiedSourceRequest.Ordinary(value) => value.approvedOriginalCreation.plan()
    val plan = MySqlBootstrapPlanIdentity(p.creationOperationUuid(), p.bootstrapOperationUuid(), p.prepareWriteUuid(),
      p.certifyWriteUuid(), p.bindingUuid(), p.mysqlServerUuid(), p.schema(), p.schemaFormat(), p.physicalFormat(), p.ddlSha256(), p.provisioningUuid())
    val stage = attempt.phase() match
      case com.wi.floatingpod.server.B1ResumeRecord.Phase.PREPARE => "PREPARE"
      case com.wi.floatingpod.server.B1ResumeRecord.Phase.CERTIFY => "CERTIFY"
      case com.wi.floatingpod.server.B1ResumeRecord.Phase.HANDOFF => "HANDOFF"
    val tw = attempt.targetWriter()
    root.reserveRetainedMemory(16384L, "full original effect/context reconstruction before canonical allocations")
    val effect = if handoff then
      val bw = attempt.beforeWriter()
      val beforeWriter = MySqlBusinessWriterRow(bw.binding(), bw.epoch(), Option(bw.ownerBoot()), bw.leaseUntil(),
        Option(bw.lastSlot()).map(_.operation()), Option(bw.lastSlot()).map(slot => slot.fingerprint().toVector), bw.leaseUntil())
      val targetWriter = MySqlBusinessWriterRow(tw.binding(), tw.epoch(), Option(tw.ownerBoot()), tw.leaseUntil(),
        Option(tw.lastSlot()).map(_.operation()), Option(tw.lastSlot()).map(slot => slot.fingerprint().toVector), tw.leaseUntil())
      MySqlOrdinaryHandoffGraph.effectFingerprint(beforeWriter, targetWriter, attempt.attemptId(), attempt.logicalWriteUuid(), root)
    else MySqlFreshFormat3Graph.effectFingerprint(stage, plan, fullBefore, before.repository, target.repository,
      MySqlFreshFormat3WriterTarget(tw.binding(), tw.epoch(), Option(tw.ownerBoot()), tw.leaseUntil(), attempt.logicalWriteUuid()))
    val expectedVersion = if handoff then attempt.beforeWriter().epoch() else 0L
    require(effects.get(0).expectedVersion() == expectedVersion && Arrays.equals(effect.toArray, effects.get(0).desiredFingerprint()),
      "original full20 effect fingerprint differs")
    val context = MySqlOperationContext(attempt.context().boot(), attempt.context().writerEpoch(), attempt.context().binding(),
      attempt.context().operation(), Vector(MySqlExpectedEffect(expectedTarget, expectedVersion, effect)))
    require(Arrays.equals(context.canonicalFingerprintSha256.toArray, attempt.context().originalOperationFingerprint()),
      "original operation fingerprint differs")
    true

  private def reject(message: String): Nothing = throw new SQLDataException(message)
  private def table(name: String): String = quotedSchema + "." + MySqlFencedTransactionSupport.quote(name)
  private def withStatement[A](sql: String)(body: PreparedStatement => A): A = MySqlBusinessRows.using(connection.prepareStatement(sql))(body)
  private def withResultSet[A](rs: ResultSet)(body: ResultSet => A): A = MySqlBusinessRows.using(rs)(body)
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


private[floatingpod] object MySqlCertifiedRecoveryInspectionActor:
  /** 仅原Actor的result槽签发；Ending不是整个退出成功，原root到实际close后才撤。 */
  final class OrdinaryBorrowRetirement private[MySqlCertifiedRecoveryInspectionActor] (
      private[mysql] var originalRoot: MySqlFormat3ReadBudget):
    private[mysql] var failure: Throwable = null
    private[mysql] var phase = 0
    private[mysql] def retainFailure(cause: Throwable): Unit =
      if failure == null then failure=cause
      else if failure ne cause then MySqlOwnedSessionLifecycle.suppress(failure,cause)

  /** Historical Recall input DATA must obey the original handoff's concrete W transition. */
  private[mysql] def requireRecallHistoricalWriterStep(attempt: B1ResumeRecord.AttemptAssertions, record: B1ResumeRecord): Unit =
    require(attempt != null && record != null && attempt.phase() == B1ResumeRecord.Phase.HANDOFF &&
      record.kind() == B1ResumeRecord.Kind.HANDOFF_PREPARED, "original HANDOFF artifact required")
    val previous = record.assertions().previousOwner()
    val owner = record.assertions().currentOwner()
    val before = attempt.beforeWriter()
    val target = attempt.targetWriter()
    require(previous != null && owner != null && previous.boot() != owner.boot() &&
      attempt.bootGeneration() == owner.generation() && attempt.context().boot() == owner.boot() &&
      target.ownerBoot() == owner.boot() && Option(before.ownerBoot()).forall(_ == previous.boot()) &&
      before.binding() == target.binding() && target.binding() == attempt.context().binding() &&
      target.epoch() == Math.incrementExact(before.epoch()) && attempt.context().writerEpoch() == target.epoch() &&
      target.leaseUntil().isAfter(before.leaseUntil()), "historical HANDOFF owner/epoch/lease transition differs")

  /** DATA-only mirror of actual Fresh targetSnapshot's exact three-relation delta. */
  def requireFreshTargetRelations(before: MySqlFormat3FullSnapshot, target: MySqlFormat3FullSnapshot,
      root: MySqlFormat3ReadBudget): Unit =
    require(before != null && target != null && root != null && !root.isClosed &&
      (before.limitProfile eq root.profile) && (target.limitProfile eq root.profile) &&
      before.allocationBudget.sharesAllocationLedger(root) && target.allocationBudget.sharesAllocationLedger(root),
      "Fresh full20 comparison requires the actual original open root/profile")
    before.relations.foreach { original =>
      if original.table != "physical_recovery_binding" && original.table != "binding_bootstrap_intent" &&
          original.table != "fp_runtime_writer" && target.relation(original.table) != original then
        throw new SQLDataException("Fresh full20 target changed a relation outside B, B1 intent, and W: " + original.table)
    }

  final class RecoveryLockedReadOwner private[MySqlCertifiedRecoveryInspectionActor] (
      actor: MySqlCertifiedRecoveryInspectionActor, gate: MySqlConnectionTransactionGate.Owner, rollback: MySqlRollbackOnlyState,
      manifest: MySqlLockedManifest, binding: Option[PhysicalRecoveryBinding], writer: MySqlBusinessWriterRow):
    private val session = actor.session
    private val verified = actor.verified
    private val scope = actor.scope
    private val expectation = actor.expectation
    private val root = actor.root
    private val held = actor.held
    private def connection = verified.connection
    val originalRetainedRoot: MySqlFormat3ReadBudget = root
    def requireCurrent(v: VerifiedMySqlConnection, s: MySqlAuditBootScope, schema: String,
        profile: com.wi.floatingpod.config.SourceLimitProfile, c: Connection): Unit =
      session.requireRecoveryActor(actor)
      require((v eq verified) && (s eq scope) && (c eq connection) && schema == expectation.schema &&
        (profile eq root.profile), "exact private certified recovery reader scope required")
      actor.requireQualification()
      MySqlConnectionTransactionGate.requireCurrent(gate, c, rollback)
      rollback.cause.foreach(throw _)
      held.readmit(verified, expectation, scope)
    def requireSnapshot(s: MySqlFormat3FullSnapshot): Unit =
      requireCurrent(verified, scope, expectation.schema, root.profile, connection)
      require(s != null && s.allocationBudget.isSnapshotReadChildOf(root) && s.allocationBudget.sharesAllocationLedger(root) &&
        s.connection == MySqlFormat3ConnectionObservation.from(verified.identity, "REPEATABLE-READ") &&
        s.observationScope == MySqlFormat3ObservationScope(scope.bootUuid, scope.bootGeneration, scope.connectionGeneration),
        "actual full20 image differs from original recovery owner")
      MySqlFreshFormat3Graph.requireManifestMatches(s, manifest, expectation.format3Inputs.get)
      MySqlFreshFormat3Graph.requireBindingMatches(s, binding)
      MySqlFreshFormat3Graph.requireWriterMatches(s, writer)
    def markFailure(cause: Throwable): Unit = rollback.mark(cause)

  /** Exact old full20 payloads are retained only when the original cold actor decoded and matched them. */
  private[floatingpod] final class VerifiedV5ArtifactSource private[MySqlCertifiedRecoveryInspectionActor] (
      private[MySqlCertifiedRecoveryInspectionActor] val actor: MySqlCertifiedRecoveryInspectionActor,
      val reference: com.wi.floatingpod.server.B1Full20WitnessReference,
      val identity: com.wi.floatingpod.server.WindowsMarkerIo.FileIdentity,
      private val accounting: MySqlFull20ColdReadAccounting,
      private val completePayload: Array[Byte],
      private val expectedAttempt: com.wi.floatingpod.server.B1ResumeRecord.AttemptAssertions
  ):
    def requireCurrent(): Unit = actor.requireV5ArtifactSource(this)
    def matchesCompletePayload(candidate: Array[Byte]): Boolean =
      requireCurrent()
      candidate != null && java.util.Arrays.equals(candidate, completePayload)
    private[MySqlCertifiedRecoveryInspectionActor] def heldAccounting = accounting
    private[MySqlCertifiedRecoveryInspectionActor] def heldPayload = completePayload
    private[MySqlCertifiedRecoveryInspectionActor] def heldAttempt = expectedAttempt
    override def toString: String = "VerifiedV5ArtifactSource[actual retained full20 bytes; no publication ACK]"

  final class VerifiedOriginalCertifiedSource private[MySqlCertifiedRecoveryInspectionActor] (
      private[mysql] val actor: MySqlCertifiedRecoveryInspectionActor,
      private[mysql] val session: MySqlOwnedDatabaseSession,
      private[mysql] val verified: VerifiedMySqlConnection,
      private[mysql] val scope: MySqlAuditBootScope,
      private[mysql] val held: HeldSessionBarrier,
      private[mysql] val request: MySqlOrdinaryCertifiedBootRequest,
      private[mysql] val oldCurrentV4: WindowsB1TwoSlotStore.ResumeStoredRecordFacts,
      private[mysql] val sourceRecord: B1ResumeRecord,
      private[mysql] val sourceRecordCanonicalBytes: Array[Byte],
      private[mysql] val oldCurrentV5: Option[B1ResumeRecordV5],
      private[mysql] val oldCurrentV5CanonicalBytes: Option[Array[Byte]],
      private[mysql] val currentV5Identity: Option[WindowsMarkerIo.FileIdentity],
      private[mysql] val root: MySqlFormat3ReadBudget,
      private[mysql] val previousOwner: B1ResumeRecord.NativeOwnerAssertions,
      private[mysql] val sourceIdentity: String,
      private[mysql] val namespace: String
  ):
    require(oldCurrentV4 != null && sourceRecord != null && sourceRecordCanonicalBytes != null &&
      sourceRecordCanonicalBytes.nonEmpty && oldCurrentV5 != null && oldCurrentV5CanonicalBytes != null &&
      currentV5Identity != null,
      "actual source V4 record plus its exact optional V5 current inventory is required")
    require(oldCurrentV5.isDefined == oldCurrentV5CanonicalBytes.isDefined &&
      oldCurrentV5.isDefined == currentV5Identity.isDefined,
      "existing V5 current requires its full DATA bytes and actual native file identity together")
    oldCurrentV5.foreach { value =>
      require(java.util.Arrays.equals(B1ResumeRecordCanonical.encode(value.transition()), sourceRecordCanonicalBytes),
        "selected V5 transition differs from the retained settled source record")
      val bytes = oldCurrentV5CanonicalBytes.get
      require(java.util.Arrays.equals(B1ResumeRecordV5.encodeData(value), bytes) &&
        value.bridgeSequence() == oldCurrentV4.record().sequence() &&
        value.bridgeIdentity().versionedIdentity() == oldCurrentV4.fileIdentity() &&
        java.util.Arrays.equals(value.bridgeSha256(), WindowsWorldProvisioner.digest(oldCurrentV4.canonicalBytes())),
        "selected V5 DATA does not continue the exact actual V4 bridge identity/full bytes")
    }
    if oldCurrentV5.isEmpty then
      require(java.util.Arrays.equals(B1ResumeRecordCanonical.encode(oldCurrentV4.record()), sourceRecordCanonicalBytes),
        "V4-only source record must be the actual selected V4 settled record")
    private[mysql] val ownerThread: Thread = Thread.currentThread()
    private[mysql] val connectionId: Long = held.actualConnectionId
    private[mysql] val connectionGeneration: UUID = held.connectionGenerationId
    private[mysql] val birthPlan: WindowsWorldProvisioner.Plan = request.approvedOriginalCreation.plan()
    private var planned: PlannedCertifiedHandoff = null
    private var failed: Throwable = null

    private[mysql] def registerPlan(value: PlannedCertifiedHandoff): Unit =
      requireOwner()
      require(value != null && (value.source eq this) && planned == null && failed == null,
        "one exact planned handoff can be retained by its source capability")
      planned = value

    private[mysql] def registerFailure(cause: Throwable): Unit =
      requireOwner()
      require(cause != null, "original ordinary workflow failure required")
      if failed == null then failed = cause
      else if failed ne cause then MySqlOwnedSessionLifecycle.suppress(failed, cause)

    private[mysql] def requireFor(actualActor: MySqlCertifiedRecoveryInspectionActor,
        actualSession: MySqlOwnedDatabaseSession, actualVerified: VerifiedMySqlConnection,
        actualScope: MySqlAuditBootScope, actualHeld: HeldSessionBarrier): Unit =
      requireOwner()
      session.requireRecoveryActor(actor)
      require((actualActor eq actor) && (actualSession eq session) && (actualVerified eq verified) &&
        (actualScope eq scope) && (actualHeld eq held) && Thread.currentThread() == ownerThread &&
        held.actualConnectionId == connectionId && held.connectionGenerationId == connectionGeneration &&
        held.namespace.lockName() == namespace && failed == null,
        "exact original ordinary actor/session/CID/generation/namespace remains current")
      held.readmit(verified, actor.expectation, scope)
      actor.requireQualification()

    private[mysql] def requireOwner(): Unit =
      require(Thread.currentThread() eq ownerThread, "ordinary source capability belongs to its actual owned worker")

    private[mysql] def retainedFailure: Option[Throwable] =
      requireOwner(); Option(failed)

    private[mysql] def pendingPlan: Option[PlannedCertifiedHandoff] =
      requireOwner(); Option(planned)

    override def toString: String = "VerifiedOriginalCertifiedSource[private actual source capability]"

  /** 只能由原actor在完整SQL/native排他源中签发；同值DATA不能制造此一次性接续。 */
  final class OriginalOrdinaryReceiptContinuation private[MySqlCertifiedRecoveryInspectionActor] (
      actor: MySqlCertifiedRecoveryInspectionActor, source: VerifiedOriginalCertifiedSource,
      before: MySqlFormat3FullSnapshot, writer: MySqlBusinessWriterRow,
      data: MySqlNormalRecallRecovery.ReceiptData):
    private var consumed = false
    private val owner = Thread.currentThread()
    private[mysql] def consume(actual: MySqlFormat3FullSnapshot, actualWriter: MySqlBusinessWriterRow,
        record: B1ResumeRecord, root: MySqlFormat3ReadBudget): B1ResumeRecord.ReceiptSlotAssertions =
      require(!consumed && (Thread.currentThread() eq owner) && (actual eq before) && (actualWriter eq writer) &&
        (record eq source.sourceRecord) && (root eq source.root) && (source eq actor.ordinarySource) &&
        source.pendingPlan.isEmpty && actor.ordinaryDraft == null, "exact original one-shot receipt continuation required")
      source.requireFor(actor, actor.session, actor.verified, actor.scope, actor.held)
      actor.native.requireSource()
      require(actor.native.previousNativeOwner == source.previousOwner &&
        actor.native.originalSourceIdentity == source.sourceIdentity && actor.native.namespace == source.namespace &&
        actor.exactV4Bytes(actor.native.selectedRecord, source.oldCurrentV4), "original receipt native source changed before handoff")
      // 已保留完整actual image与核验结果；这里只读同一不可变对象，不重新取得可替换的caller行集。
      val context = data.context
      require(writer.lastOperationUuid.contains(context.operationUuid) &&
        writer.lastFingerprint.contains(data.receiptFingerprint), "original checked W receipt changed")
      root.reserveRetainedMemory(512L, "continued original receipt slot")
      consumed = true
      new B1ResumeRecord.ReceiptSlotAssertions(context.bootUuid, context.writerEpoch, context.bindingUuid,
        context.operationUuid, data.receiptFingerprint.toArray)
    override def toString: String = "OriginalOrdinaryReceiptContinuation[private original actor]"

  final class PlannedCertifiedHandoff private[MySqlCertifiedRecoveryInspectionActor] (
      private[mysql] val source: VerifiedOriginalCertifiedSource,
      private[mysql] val attempt: B1ResumeRecord.AttemptAssertions,
      private[mysql] val context: MySqlOperationContext,
      private[mysql] val before: MySqlFormat3FullSnapshot,
      private[mysql] val target: MySqlFormat3FullSnapshot,
      private[mysql] val beforeWriter: MySqlBusinessWriterRow,
      private[mysql] val targetWriter: MySqlBusinessWriterRow,
      private[mysql] val beforeWriterAssertions: B1ResumeRecord.WriterAssertions,
      private[mysql] val targetWriterAssertions: B1ResumeRecord.WriterAssertions,
      private[mysql] val beforeB1Graph: MySqlB1CommonGraph,
      private[mysql] val targetB1Graph: MySqlB1CommonGraph,
      private[mysql] val beforeCommon: B1ResumeRecord.CommonGraphAssertions,
      private[mysql] val targetCommon: B1ResumeRecord.CommonGraphAssertions,
      private[mysql] val targetState: B1ResumeRecord.FullStateAssertions,
      private[mysql] val targetTransition: B1ResumeRecord,
      private[mysql] val canonicalBytes: Array[Byte],
      private[mysql] val targetParity: Int,
      private[mysql] val leaseDeadline: LocalDateTime
  ):
    private var publicationAttempted = false
    private var installed = false
    private var sqlCommitAttempted = false
    private var sqlCommitAcknowledged = false
    private var primary: Throwable = null
    private var nativeAcknowledgement: OrdinaryNativeHandoffAcknowledgement = null

    private[mysql] def beginPublication(actual: VerifiedOriginalCertifiedSource): Unit =
      source.requireOwner()
      require((actual eq source) && !publicationAttempted && primary == null && canonicalBytes.nonEmpty,
        "planned HANDOFF native publication is sticky and exact")
      publicationAttempted = true

    private[mysql] def retainNativeAcknowledgement(value: OrdinaryNativeHandoffAcknowledgement): Unit =
      source.requireOwner()
      require(publicationAttempted && nativeAcknowledgement == null && value != null,
        "exact actual HANDOFF ACK retained once")
      nativeAcknowledgement = value

    private[mysql] def retainInstalled(): Unit =
      source.requireOwner()
      require(nativeAcknowledgement != null && !installed && primary == null,
        "actual installed custody can follow only the exact HANDOFF ACK")
      installed = true

    private[mysql] def beginSqlCommit(): Unit =
      source.requireOwner()
      require(installed && !sqlCommitAttempted && primary == null,
        "ordinary W SQL mutation is exact and sticky")
      sqlCommitAttempted = true

    private[mysql] def acknowledgedSqlCommit(): Unit =
      source.requireOwner()
      require(sqlCommitAttempted && !sqlCommitAcknowledged && primary == null,
        "ordinary W SQL commit acknowledgement is retained once")
      sqlCommitAcknowledged = true

    private[mysql] def fail(cause: Throwable): Unit =
      source.requireOwner()
      require(cause != null, "original ordinary plan failure required")
      if primary == null then primary = cause
      else if primary ne cause then MySqlOwnedSessionLifecycle.suppress(primary, cause)
      source.registerFailure(cause)

    private[mysql] def requirePublishedAndInstalled(): Unit =
      source.requireOwner()
      require(publicationAttempted && nativeAcknowledgement != null && installed && primary == null,
        "actual HANDOFF publication/ACK/install chain required")

    private[mysql] def requireCommitted(): Unit =
      requirePublishedAndInstalled()
      require(sqlCommitAttempted && sqlCommitAcknowledged, "exact acknowledged W CAS/commit required")

    private[mysql] def actualNativeAcknowledgement: OrdinaryNativeHandoffAcknowledgement =
      source.requireOwner(); nativeAcknowledgement

    private[mysql] def retainedFailure: Option[Throwable] =
      source.requireOwner(); Option(primary)

    private[mysql] def isCommitAttempted: Boolean = { source.requireOwner(); sqlCommitAttempted }
    private[mysql] def isCommitAcknowledged: Boolean = { source.requireOwner(); sqlCommitAcknowledged }

    override def toString: String = "PlannedCertifiedHandoff[private actual W delta and native custody]"

  final class CommittedCertifiedHandoff private[MySqlCertifiedRecoveryInspectionActor] (
      private[mysql] val plan: PlannedCertifiedHandoff,
      private[mysql] val targetSnapshot: MySqlFormat3FullSnapshot,
      private[mysql] val targetWriter: MySqlBusinessWriterRow,
      private[mysql] val actualCurrentV5: Option[B1ResumeRecordV5],
      private[mysql] val retainedNativeAcknowledgement: OrdinaryNativeHandoffAcknowledgement,
      private[mysql] val postCommitObservedAtNanos: Long,
      private val commitEvidence: Array[Byte],
      private val originalUnknownDiagnosticIdentity: Array[Byte]
  ):
    private val ownerThread = Thread.currentThread()
    require(plan != null && targetSnapshot != null && targetWriter != null && retainedNativeAcknowledgement != null &&
      postCommitObservedAtNanos != 0L && commitEvidence != null && commitEvidence.nonEmpty && commitEvidence.length <= 65536 &&
      originalUnknownDiagnosticIdentity != null && originalUnknownDiagnosticIdentity.length == 32,
      "same-actor committed W target, post-observation, and exact durable commit evidence are required")
    private val retainedCommitEvidence = commitEvidence.clone()
    private val retainedUnknownDiagnosticIdentity = originalUnknownDiagnosticIdentity.clone()
    private var completed: CompletedCertifiedHandoff = null
    private var primary: Throwable = null
    private[mysql] def requireForNativeContinuation(
        flight: WindowsB1V5WitnessRecordStore.VerifiedHandoffRetirementFlight,
        installed: WindowsB1TwoSlotStore.InstalledResumeRecordScope
    ): Unit =
      require(Thread.currentThread() eq ownerThread, "exact committed ordinary actor thread required")
      plan.source.actor.requireCommittedNativeContinuation(this, flight, installed)
    private[mysql] def requireSettledRecord(value: B1ResumeRecord): Unit =
      require(Thread.currentThread() eq ownerThread, "exact committed ordinary actor thread required")
      plan.source.actor.requireSettledNativeRecord(this, value)
    private[mysql] def requireOriginalV4Predecessor(
        record: B1ResumeRecord, canonical: Array[Byte], fileIdentity: String
    ): Unit =
      require(Thread.currentThread() eq ownerThread, "exact committed ordinary actor thread required")
      plan.source.actor.requireOriginalV4Predecessor(this, record, canonical, fileIdentity)
    private[mysql] def commitEvidenceCopy: Array[Byte] =
      require(Thread.currentThread() eq ownerThread, "exact committed ordinary actor thread required")
      retainedCommitEvidence.clone()
    private[mysql] def unknownDiagnosticIdentityCopy: Array[Byte] =
      require(Thread.currentThread() eq ownerThread, "exact committed ordinary actor thread required")
      retainedUnknownDiagnosticIdentity.clone()
    private[mysql] def markCompleted(value: CompletedCertifiedHandoff): Unit =
      require(Thread.currentThread() eq plan.source.ownerThread, "exact ordinary actor thread required")
      require(value != null && (value.committed eq this) && completed == null && primary == null,
        "ordinary post-audit/native completion is retained once")
      plan.requireCommitted()
      completed = value
    private[mysql] def fail(cause: Throwable): Unit =
      require(Thread.currentThread() eq plan.source.ownerThread, "exact ordinary actor thread required")
      if primary == null then primary = cause else if primary ne cause then MySqlOwnedSessionLifecycle.suppress(primary, cause)
      plan.fail(cause)
    private[mysql] def requireCompleted(value: CompletedCertifiedHandoff): Unit =
      require((Thread.currentThread() eq plan.source.ownerThread) && primary == null && completed != null && (completed eq value),
        "exact completed ordinary handoff remains current")
    private[mysql] def retainedFailure: Option[Throwable] =
      require(Thread.currentThread() eq plan.source.ownerThread); Option(primary)
    override def toString: String = "CommittedCertifiedHandoff[private actual SQL/native post-commit state]"

  /** 只能由原actor在实际native/W handoff读回后发行；不包含business升级入口。 */
  final class RecallRecoveryAdmission private[MySqlCertifiedRecoveryInspectionActor] (
      private[mysql] val actor: MySqlCertifiedRecoveryInspectionActor,
      private[mysql] val completed: CompletedCertifiedHandoff,
      private[mysql] val source: VerifiedOriginalCertifiedSource,
      private[mysql] val snapshot: MySqlFormat3FullSnapshot,
      private[mysql] val inventory: Vector[MySqlNormalRecallRecoveryInventory],
      private val checkpointDecisions: Vector[MySqlNormalRecallRecovery.CheckpointDecisionData]):
    private[MySqlCertifiedRecoveryInspectionActor] var currentToken = completed.token
    private[MySqlCertifiedRecoveryInspectionActor] var currentReceipt = completed.receipt
    private[mysql] def requireCurrent(): Unit = actor.requireRecallRecovery(this)
    private[mysql] def token: MySqlWriterFenceToken = { requireCurrent(); currentToken }
    private[mysql] def receipt: RuntimeSchemaAuditReceipt = { requireCurrent(); currentReceipt }
    private[mysql] def checkpointObservation(original: MySqlNormalRecallRecoveryInventory): MySqlNormalRecallCheckpointObservation =
      requireCurrent()
      require(inventory.exists(_ eq original), "C observation requires this original admission inventory")
      val found = checkpointDecisions.filter(_.inventory eq original)
      require(found.size == 1, "original private C decision holder missing or ambiguous")
      found.head.observation
    override def toString: String = "RecallRecoveryAdmission[private original owner; no business or physical authority]"

  final class CompletedCertifiedHandoff private[MySqlCertifiedRecoveryInspectionActor] (
      private[mysql] val committed: CommittedCertifiedHandoff,
      private[mysql] val actor: MySqlCertifiedRecoveryInspectionActor,
      private[mysql] val verified: VerifiedMySqlConnection,
      private[mysql] val scope: MySqlAuditBootScope,
      private[mysql] val token: MySqlWriterFenceToken,
      private[mysql] val receipt: RuntimeSchemaAuditReceipt,
      val facts: MySqlOrdinaryFacts,
      private[mysql] val settledNativeAcknowledgement: OrdinaryNativeSettledAcknowledgement,
      private[mysql] val preAnvilProjection: MySqlOrdinaryPreAnvilProjection
  ):
    private[mysql] def validateFor(expected: MySqlSchemaAuditExpectation,
        coordinator: MySqlWriterFenceCoordinator): Unit =
      require(committed != null && actor != null && verified != null && scope != null && token != null &&
        receipt != null && facts != null && settledNativeAcknowledgement != null && preAnvilProjection != null,
        "complete ordinary actual completion proof is required")
      actor.validateCompletedHandoff(this, expected, coordinator)
    override def toString: String = "CompletedCertifiedHandoff[private same-actor DB/native completion proof]"

  /** A projection-only custody value. It does not authorize managed saves or GameReady. */
  final class MySqlOrdinaryPreAnvilProjection private[MySqlCertifiedRecoveryInspectionActor] (
      private[mysql] val actor: MySqlCertifiedRecoveryInspectionActor,
      private[mysql] val source: MySqlCertifiedOrdinaryCapabilities.VerifiedOriginalCertifiedSource,
      private[mysql] val installedWorld: WindowsWorldProvisioner.ProvisionedWorld
  ):
    require(actor != null && source != null && installedWorld != null, "actual ordinary installed custody projection required")
    override def toString: String = "OrdinaryPreAnvilProjection[actual IO source custody; no save permission]"
