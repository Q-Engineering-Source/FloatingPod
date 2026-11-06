package com.wi.floatingpod.storage.mysql

import java.time.Duration
import java.util.UUID
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.config.SourceLimitProfile
import scala.util.{Failure, Success, Try}

enum MySqlOwnedSessionRelease:
  case NoAcquireAttempt
  case RetainedFailure
  case RetainedNotAdmitted
  case RetainedAdmissionUnavailable(cause: Throwable)
  case Released
  case ReleaseFailed(cause: Throwable)

/** 只记录本 holder 的调用/实际 JDBC getter 观察，绝不是 physical Ready 或 IOExitProof。 */
final case class MySqlOwnedSessionCloseObservation(
    bootId: UUID, bootGeneration: Long, connectionGeneration: Long,
    connectionOpened: Boolean, acquireAttempted: Boolean,
    release: MySqlOwnedSessionRelease, releaseAttempted: Boolean, releaseReturned: Boolean,
    jdbcCloseAttempted: Boolean, jdbcClosed: Option[Boolean],
    frozenFailure: Option[Throwable], failure: Option[Throwable],
    freshResponsibility: Option[MySqlFreshBootstrapResponsibility] = None,
    sessionBarrier: Option[MySqlSessionBarrierCloseObservation] = None,
    normalResponsibility: Option[MySqlNormalBootstrapResponsibility] = None,
    certifiedInspection: Option[MySqlCertifiedInspectionObservation] = None
):
  def unresolvedOwnedResponsibility: Boolean =
    (acquireAttempted && !releaseReturned) || (connectionOpened && !jdbcClosed.contains(true)) ||
      freshResponsibility.exists(_.retainedResponsibility) || normalResponsibility.exists(_.retainedResponsibility) || sessionBarrier.exists(_.mayRetainServerLock) || certifiedInspection.exists(r => r.retainedNativeResponsibility || r.originalRootRetained)
  override def toString: String = "MySqlOwnedSessionCloseObservation[redacted; not IOExitProof]"

final case class MySqlOwnedDatabaseAdmission(
    bootId: UUID, bootGeneration: Long, connectionGeneration: Long, bindingId: UUID, writerEpoch: Long
)

/** 仅附 suppressed 的收口上下文，不替换原 Throwable 类型/identity，也不建立反向 cause 环。 */
final class MySqlOwnedSessionInitializationObservation(
    val stage: String, val closeObservation: MySqlOwnedSessionCloseObservation,
    private[mysql] val retainedSession: MySqlOwnedDatabaseSession = null
) extends IllegalStateException("owned MySQL session initialization observation at " + stage)

/** 单一实际 owner 的 actor gate；无 JDBC success seam，也不授予 admission。 */
private[mysql] final class MySqlOwnedSessionLifecycle(owner: Thread):
  require(owner != null, "actual owned IO thread required")
  private var opening = false
  private var admitted = false
  private var recallRecovering = false
  private var closing = false
  private var terminated = false
  private var primary = Option.empty[Throwable]

  def requireOwner(): Unit =
    require(Thread.currentThread() eq owner, "owned MySQL session requires exact actual IO thread")

  def beginOpen(): Unit =
    requireOwner()
    require(!opening && !closing && !terminated, "owned session open is one-shot")
    opening = true

  def admit(): Unit =
    requireOwner()
    require(opening && !recallRecovering && !closing && !terminated && primary.isEmpty,
      "owned session cannot admit across unresolved Recall recovery, failure or shutdown")
    admitted = true

  /** 只记录actor生命周期；实际许可仍须Session保留的私有原issuer capability。 */
  private[mysql] def beginRecallRecovery(): Unit =
    requireOwner()
    require(opening && !admitted && !recallRecovering && !closing && !terminated && primary.isEmpty,
      "Recall recovery requires an unadmitted live opening session")
    recallRecovering = true

  private[mysql] def requireRecallRecovering(): Unit =
    requireOwner()
    primary.foreach(throw _)
    require(recallRecovering && !admitted && !closing && !terminated,
      "original session is not in restricted Recall recovery")

  def requireAdmitted(): Unit =
    requireOwner()
    require(admitted && !closing && !terminated && primary.isEmpty, "owned session is not healthy admitted")

  def markFailed(cause: Throwable): Unit =
    requireOwner()
    require(cause != null && !terminated, "failure requires original cause before terminal shutdown")
    primary = MySqlOwnedSessionLifecycle.combine(primary, cause)

  /** worker primary 优先；旧单次失败仍 suppressed。该入口不把 None 当作已核健康证明。 */
  def beginClose(workerPrimary: Option[Throwable]): Unit =
    requireOwner()
    require(workerPrimary != null && workerPrimary.forall(_ != null), "worker failure option is malformed")
    require(!closing && !terminated, "owned session shutdown is one-shot")
    workerPrimary.foreach { fatal =>
      primary.foreach { previous => MySqlOwnedSessionLifecycle.suppress(fatal, previous) }
      primary = Some(fatal)
    }
    closing = true

  def releaseEligible: Boolean =
    requireOwner()
    require(closing && !terminated, "release decision requires shutdown actor")
    admitted && primary.isEmpty

  def failure: Option[Throwable] =
    requireOwner()
    primary

  def finishClose(): Unit =
    requireOwner()
    require(closing && !terminated, "shutdown terminal observation is one-shot")
    terminated = true

private[mysql] object MySqlOwnedSessionLifecycle:
  def suppress(primary: Throwable, secondary: Throwable): Unit =
    if (primary ne secondary) && !primary.getSuppressed.contains(secondary) then primary.addSuppressed(secondary)
  def combine(primary: Option[Throwable], secondary: Throwable): Option[Throwable] =
    primary match
      case Some(value) => suppress(value, secondary); primary
      case None => Some(secondary)

/** 正常CERTIFIED与显式Fresh B1分开初始化；Fresh只由真实typed service的opaque proof转入。
  * 从真实 source.open 成功返回起，本 holder 独占同连接/审计scope/coordinator/repository资源。
  * 所有方法均只在 actual owned IO thread 使用；不持 holder monitor 调用 SQL/close。
  */
final class MySqlOwnedDatabaseSession private (
    inputs: MySqlRuntimeInputs, connectionGeneration: Long, leaseDuration: Duration,
    freshRequest: Option[MySqlFreshBootRequest] = None,
    normalRequest: Option[MySqlNormalBootRequest] = None,
    certifiedRequest: Option[CertifiedSourceRequest] = None,
    private val v2Lease: MySqlRuntimeInputs.ConsumerLease = null,
    adoptedLedger: MySqlFormat3AllocationLedger = null
):
  private val owner = inputs.ownedIoThread
  // Boot inventory与所有业务planning roots共用实际source profile cap；消费Boot不自动释放。
  private val sharedAllocationLedger =
    if inputs.hasV2Origin then
      require(v2Lease != null && adoptedLedger != null,"V2 Session requires caller-retained lease and actual ledger")
      v2Lease.requireFor(inputs,adoptedLedger)
      Some(adoptedLedger)
    else
      require(v2Lease == null && adoptedLedger == null,"legacy Session cannot adopt V2 custody")
      inputs.auditExpectation.format3Inputs.map(value => new MySqlFormat3AllocationLedger(value.limitProfile.maxRetainedBytes()))
  private var renewSequence = 0L
  private val lifecycle = new MySqlOwnedSessionLifecycle(owner)
  // Fresh factory只分配无native/SQL side effect的holder；adapter先发布本session，之后才初始化。
  require(freshRequest.isEmpty || normalRequest.isEmpty, "Fresh and Normal boot are mutually exclusive")
  private val freshNative = freshRequest.map(request => new MySqlFreshNativeHolder(owner, request))
  private val normalNative = normalRequest.map(request => new MySqlNormalNativeHolder(owner, request))
  require(certifiedRequest.isEmpty || (freshRequest.isEmpty && normalRequest.isEmpty), "certified source boot is a distinct purpose")
  // No side effects in allocation. Session/actor/native holder are registered before open.
  private val recoveryActor = certifiedRequest.map(request => new MySqlCertifiedRecoveryInspectionActor(this, inputs,
    sharedAllocationLedger.getOrElse(throw new IllegalStateException("inspection requires actual session shared ledger")), request))
  private var inspectionResult = Option.empty[MySqlCertifiedInspectionObservation]
  private var ordinaryCompletion = Option.empty[MySqlCertifiedOrdinaryCapabilities.CompletedCertifiedHandoff]
  private var ordinaryFactsValue: MySqlOrdinaryFacts | MySqlOwnedIoAdapter.OrdinaryBootResultBacking | Null = null
  private var ordinaryCompletionClaimed = false
  private var recallRecovery = Option.empty[MySqlCertifiedOrdinaryCapabilities.RecallRecoveryAdmission]
  private var issuedFreshNativeExit: MySqlCertifiedFreshExitPermit | MySqlCertifiedOrdinaryNativeExitPermit | Null = null
  /** native consumer 再核同一 session；只接受正常 finalizer 实际登记的原权限。 */
  def requireCertifiedFreshNativeExitPermit(permit: MySqlCertifiedFreshExitPermit): Unit =
    lifecycle.requireOwner()
    require(permit != null && (permit eq issuedFreshNativeExit) && lifecycle.failure.isEmpty &&
      freshRequest.nonEmpty && originalBootReadSource == null && originalBootReadProjection == null &&
      freshCompletionPrepared && freshBootRootClosed && freshNativeReleaseEligible &&
      freshService != null && freshService.certifiedWitnessExitComplete,
      "actual settled Fresh session native exit authority required")

  private[mysql] def requireCertifiedOrdinaryNativeExitPermit(permit: MySqlCertifiedOrdinaryNativeExitPermit): Unit =
    lifecycle.requireOwner()
    if permit == null || (issuedFreshNativeExit ne permit) ||
        (certifiedRequest.isEmpty || !certifiedRequest.get.isInstanceOf[CertifiedSourceRequest.Ordinary]) || closeObserved.nonEmpty then
      throw new IllegalStateException("exact original Session-issued Ordinary native permit required")
    if lifecycle.failure.nonEmpty then throw lifecycle.failure.get

  private[mysql] def requireOrdinarySqlSettlement(permit: MySqlCertifiedOrdinaryNativeExitPermit): Unit =
    // 仅同performClose的实际release/isClosed/barrier成功分支推进；不发行可传入的成功布尔值。
    requireCertifiedOrdinaryNativeExitPermit(permit)
    if verified == null || admission == null || inRepository then
      throw new IllegalStateException("original closing Ordinary Session responsibility required")

  private[mysql] def requireOrdinaryBootBorrowRetirement(budget: MySqlFormat3ReadBudget,
      permit: MySqlCertifiedOrdinaryNativeExitPermit): Unit =
    permit.requireNativeClosed(this)
    if originalBootReadSource == null || originalBootReadProjection == null ||
        (originalBootReadSource.allocationBudget ne budget) || budget.isClosed then
      throw new IllegalStateException("exact retained original Ordinary Boot root required")

  private[mysql] def requireRecoveryActor(actor: MySqlCertifiedRecoveryInspectionActor): Unit =
    lifecycle.requireOwner()
    require(recoveryActor.exists(_ eq actor) && closeObserved.isEmpty, "exact registered live inspection actor required")

  private[mysql] def requireCertifiedOrdinaryCoordinator(
      actor: MySqlCertifiedRecoveryInspectionActor,
      candidate: MySqlWriterFenceCoordinator
  ): Unit =
    requireRecoveryActor(actor)
    require(coordinator != null && (coordinator eq candidate) && verified != null && scope != null,
      "exact same-session ordinary writer coordinator and connection scope are required")

  private[mysql] def beginCertifiedOrdinaryHandoff(actor: MySqlCertifiedRecoveryInspectionActor): Unit =
    requireRecoveryActor(actor)
    require(coordinator != null && verified != null && scope != null,
      "actual ordinary writer coordinator and registered connection scope are required")
    coordinator.beginCertifiedOrdinaryHandoff(actor, verified, scope)

  private[mysql] def certifiedOrdinaryLeaseDeadline(
      actor: MySqlCertifiedRecoveryInspectionActor,
      databaseNow: java.time.LocalDateTime
  ): java.time.LocalDateTime =
    requireRecoveryActor(actor)
    require(coordinator != null, "actual ordinary writer coordinator is required")
    coordinator.certifiedOrdinaryLeaseDeadline(actor, databaseNow)

  private[mysql] def auditCommittedCertifiedHandoff(
      actor: MySqlCertifiedRecoveryInspectionActor,
      qualification: VerifiedSchemaQualification,
      targetWriter: MySqlBusinessWriterRow
  ): (MySqlWriterFenceToken, RuntimeSchemaAuditReceipt) =
    requireRecoveryActor(actor)
    require(coordinator != null && verified != null && scope != null,
      "actual ordinary writer coordinator and registered connection scope are required")
    coordinator.auditCommittedCertifiedHandoff(actor, verified, scope, qualification, targetWriter)

  private[mysql] def failCertifiedOrdinaryHandoff(
      actor: MySqlCertifiedRecoveryInspectionActor,
      cause: Throwable
  ): Unit =
    lifecycle.requireOwner()
    if coordinator != null then coordinator.failCertifiedOrdinaryHandoff(actor, cause)

  private[mysql] def initializeCertifiedInspection(): MySqlCertifiedInspectionObservation =
    lifecycle.beginOpen()
    val actor = recoveryActor.getOrElse(throw new IllegalStateException("registered inspection actor required"))
    var stage = "certified-inspection-open"
    try
      verified = openRegisteredConnection(new MySqlConnectionSource(inputs.settings))
      scope = MySqlAuditBootScope.forOwnedIo(verified, owner, inputs.bootId, inputs.bootGeneration, connectionGeneration)
      val auditor = inputs.newAuditor()
      stage = "certified-inspection-static-audit"
      val qualification = auditor.auditBeforeWriterAcquire(auditor.readCurrentObjectManifest(verified, scope), scope)
      require(qualification.physicalBinding == MySqlCertifiedPhysicalBinding(inputs.auditExpectation.bindingUuid, 2),
        "certified inspection requires actual existing physical binding")
      stage = "certified-inspection-barrier"
      val held = verified.acquireSessionBarrier(inputs.auditExpectation, scope, Duration.ZERO) match
        case MySqlSessionBarrierAcquisition.Held(value) => value
        case MySqlSessionBarrierAcquisition.Deferred => throw new java.sql.SQLTransientConnectionException("certified inspection barrier busy; no claim")
      heldSessionBarrier = Some(held)
      actor.attach(verified, scope, held, qualification)
      stage = "certified-inspection-native-locked-read-comparison"
      val observed = actor.inspect()
      inspectionResult = Some(observed)
      observed // Never lifecycle.admit / writer.acquire / business repository.
    catch
      case primary: Throwable =>
        lifecycle.markFailed(primary)
        freshPartialClosed = true // reuse exact existing partial-close hook responsibility; no second close.
        val observed =
          try performClose(None)
          catch
            case cleanup: Throwable =>
              MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
              throw primary
        MySqlOwnedSessionLifecycle.suppress(primary, new MySqlOwnedSessionInitializationObservation(stage, observed, this))
        throw primary

  /** Ordinary restart uses the same registered actor/session/barrier and adopts only its private native completion. */
  private[mysql] def initializeCertifiedOrdinary(): MySqlOrdinaryFacts =
    initializeCertifiedOrdinaryWithVerifiedConnectionSource(() => openRegisteredConnection(new MySqlConnectionSource(inputs.settings)))

  /** Package-private owned-worker assembly seam; production keeps the concrete source.open path above. */
  private[mysql] def initializeCertifiedOrdinaryWithVerifiedConnectionSource(
      openVerifiedConnection: () => VerifiedMySqlConnection
  ): MySqlOrdinaryFacts =
    require(openVerifiedConnection != null, "ordinary verified-connection source is required")
    lifecycle.beginOpen()
    val actor = recoveryActor.getOrElse(throw new IllegalStateException("registered ordinary certified actor required"))
    require(certifiedRequest.exists(_.isInstanceOf[CertifiedSourceRequest.Ordinary]),
      "ordinary certified request is required")
    var stage = "ordinary-certified-connection-open"
    try
      verified = openVerifiedConnection()
      require(verified != null, "ordinary source must return a real verified MySQL connection")
      scope = MySqlAuditBootScope.forOwnedIo(verified, owner, inputs.bootId, inputs.bootGeneration, connectionGeneration)
      val auditor = inputs.newAuditor()
      coordinator = new MySqlWriterFenceCoordinator(inputs.auditExpectation, auditor, scope, leaseDuration)
      stage = "ordinary-certified-static-format3-audit"
      val observed = auditor.readCurrentObjectManifest(verified, scope)
      val qualification = auditor.auditBeforeWriterAcquire(observed, scope)
      require(qualification.physicalBinding == MySqlCertifiedPhysicalBinding(inputs.auditExpectation.bindingUuid, 2),
        "ordinary Certified restart requires the actual approved format3/physical2 CERTIFIED binding")
      stage = "ordinary-certified-session-termination-barrier"
      val held = verified.acquireSessionBarrier(inputs.auditExpectation, scope, Duration.ZERO) match
        case MySqlSessionBarrierAcquisition.Held(value) => value
        case MySqlSessionBarrierAcquisition.Deferred => throw new java.sql.SQLTransientConnectionException(
          "ordinary Certified session barrier is busy; no native or SQL mutation")
      heldSessionBarrier = Some(held)
      actor.attach(verified, scope, held, qualification)
      stage = "ordinary-certified-source-and-settled-handoff"
      ordinaryCompletion = Some(actor.initializeOrdinary())
      stage = "ordinary-certified-private-post-audit-native-adoption"
      ordinaryFactsValue = ordinaryCompletion.get.facts
      recallRecovery = actor.originalRecallRecovery(ordinaryCompletion.get)
      recallRecovery match
        case Some(original) =>
          original.requireCurrent()
          lifecycle.beginRecallRecovery()
        case None =>
          admission = onceClaimCompletedCertifiedHandoff(ordinaryCompletion.get)
          stage = "ordinary-certified-five-trait-repository"
          repository = new MySqlFloatingPodRepository(inputs.auditExpectation, coordinator, verified, scope, sharedAllocationLedger)
          lifecycle.admit()
      ordinaryFactsValue match
        case facts: MySqlOrdinaryFacts => facts
        case _ => throw new IllegalStateException("Ordinary initialization facts phase required")
    catch
      case primary: Throwable =>
        lifecycle.markFailed(primary)
        freshPartialClosed = true
        val observation =
          try performClose(None)
          catch
            case cleanup: Throwable =>
              MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
              throw primary
        MySqlOwnedSessionLifecycle.suppress(primary, new MySqlOwnedSessionInitializationObservation(stage, observation, this))
        throw primary

  /** Consumes the exact actor completion once; it cannot be rebuilt from published DATA facts. */
  private def onceClaimCompletedCertifiedHandoff(
      completed: MySqlCertifiedOrdinaryCapabilities.CompletedCertifiedHandoff
  ): MySqlBusinessAdmission =
    lifecycle.requireOwner()
    val actor = recoveryActor.getOrElse(throw new IllegalStateException("registered ordinary actor required"))
    require(!ordinaryCompletionClaimed && ordinaryCompletion.contains(completed),
      "the exact completed ordinary handoff is consumed once")
    ordinaryCompletionClaimed = true
    actor.claimCompletedCertifiedHandoff(completed)
    coordinator.admitPlannedCertifiedHandoff(completed)

  private var freshService: MySqlFreshWorldBootstrap = null
  private var freshLastContext = Option.empty[MySqlOperationContext]
  private var freshBarrierAcquireAttempted = false
  private var freshPartialClosed = false
  private var verified: VerifiedMySqlConnection = null
  private var openHolder: MySqlConnectionSource.SourceUnknownInspectionOpen = null

  private def openRegisteredConnection(source: MySqlConnectionSource): VerifiedMySqlConnection =
    lifecycle.requireOwner()
    require(source != null && openHolder == null && verified == null, "original Session connection open is one-shot")
    openHolder = source.newOwnedSessionOpen()
    openHolder.openOnce()

  private[mysql] def retainedPartialOpen: MySqlConnectionSource.SourceUnknownInspectionOpen =
    lifecycle.requireOwner()
    openHolder

  private var scope: MySqlAuditBootScope = null
  private var coordinator: MySqlWriterFenceCoordinator = null
  private var repository: MySqlFloatingPodRepository = null
  private var admission: MySqlBusinessAdmission = null
  // 保留实际 SQL issuer 的同一 receipt；CID/generation DTO 不能重建持有权。
  private var heldSessionBarrier = Option.empty[HeldSessionBarrier]
  private var acquireAttempted = false
  private var inRepository = false
  private var borrowGeneration = 0L
  private var activeBorrow = Option.empty[MySqlOwnedRepositoryBorrow]
  private var closeObserved = Option.empty[MySqlOwnedSessionCloseObservation]

  override def toString: String = "MySqlOwnedDatabaseSession[redacted; database owner only]"

  private def initialize(): Unit =
    initializeWithVerifiedConnectionSource(() => openRegisteredConnection(new MySqlConnectionSource(inputs.settings)))

  /** Same certified lifecycle; assembly supplies only a verifier-issued connection, never admission. */
  private def initializeWithVerifiedConnectionSource(openVerifiedConnection: () => VerifiedMySqlConnection): Unit =
    require(openVerifiedConnection != null, "certified verified-connection source is required")
    lifecycle.beginOpen()
    var stage = "connection-open"
    try
      // 此调用只打开显式 MYSQL；返回前连接仍由 source 负责，返回后立刻登记本 holder 所有权。
      verified = openVerifiedConnection()
      stage = "owned-scope"
      scope = MySqlAuditBootScope.forOwnedIo(verified, owner, inputs.bootId, inputs.bootGeneration, connectionGeneration)
      val auditor = inputs.newAuditor()
      coordinator = new MySqlWriterFenceCoordinator(inputs.auditExpectation, auditor, scope, leaseDuration)
      stage = "session-termination-barrier"
      verified.acquireSessionBarrier(inputs.auditExpectation, scope, Duration.ZERO) match
        case MySqlSessionBarrierAcquisition.Held(value) => heldSessionBarrier = Some(value)
        case MySqlSessionBarrierAcquisition.Deferred =>
          throw new java.sql.SQLTransientConnectionException("MySQL source session barrier is busy; deferred")
      stage = "certified-fence-acquire"
      acquireAttempted = true
      val token = coordinator.acquire(verified, scope) // 真 pre-audit/同连接 acquire；不认可 None/Legacy 为业务权利。
      stage = "post-acquire-audit"
      admission = coordinator.auditAndConfirmPostAcquire(token)
      stage = "five-trait-repository"
      repository = new MySqlFloatingPodRepository(inputs.auditExpectation, coordinator, verified, scope, sharedAllocationLedger)
      lifecycle.admit()
    catch
      case primary: Throwable =>
        lifecycle.markFailed(primary)
        val observation = performClose(None)
        MySqlOwnedSessionLifecycle.suppress(primary, new MySqlOwnedSessionInitializationObservation(stage, observation, this))
        throw primary

  /** package-only；adapter必须先把allocateFresh返回值登记到sessionRef，再调用此方法。 */
  private[mysql] def initializeFresh(): Unit =
    initializeFreshWithVerifiedConnectionSource(() => openRegisteredConnection(new MySqlConnectionSource(inputs.settings)))

  /** Test-only assembly preserves the same actual Fresh actor/lifecycle and accepts only the verifier's issuer type. */
  private[mysql] def initializeFreshWithVerifiedConnectionSource(
      openVerifiedConnection: () => VerifiedMySqlConnection
  ): Unit =
    require(openVerifiedConnection != null, "Fresh verified-connection source is required")
    lifecycle.beginOpen()
    require(freshNative.nonEmpty, "explicit Fresh owner holder required")
    var stage = "fresh-connection-open"
    try
      verified = openVerifiedConnection()
      require(verified != null, "Fresh source must return a real verified MySQL connection")
      stage = "fresh-owned-scope"
      scope = MySqlAuditBootScope.forOwnedIo(verified, owner, inputs.bootId, inputs.bootGeneration, connectionGeneration)
      val auditor = inputs.newAuditor()
      coordinator = new MySqlWriterFenceCoordinator(inputs.auditExpectation, auditor, scope, leaseDuration)
      // 当前typed static+READY/noBinding资格在任何nativecreate前；ordinary acquire仍不参与Fresh。
      stage = "fresh-current-static-ready-audit"
      val observed = auditor.readCurrentObjectManifest(verified, scope)
      val compared = auditor.compareRequiredObjectManifest(observed)
      val qualification = auditor.auditBeforeWriterAcquire(observed, scope)
      require(compared.actualCanonicalSha256 == qualification.objectManifestSha256 &&
        qualification.physicalBinding == MySqlNoPhysicalBinding, "Fresh requires current approved READY/noBinding before native create")
      stage = "fresh-session-termination-barrier"
      freshBarrierAcquireAttempted = true
      val held = verified.acquireSessionBarrier(inputs.auditExpectation, scope, Duration.ZERO) match
        case MySqlSessionBarrierAcquisition.Held(value) => value
        case MySqlSessionBarrierAcquisition.Deferred =>
          throw new java.sql.SQLTransientConnectionException("Fresh source session barrier is busy; deferred before native create")
      heldSessionBarrier = Some(held)
      stage = "fresh-native-source-capture-create-anchor"
      val native = freshNative.get
      if inputs.hasV2Origin then inputs.assemblyOwnerV2.withConfigurationWorkspace {
        native.createFreshV2(inputs.originalMetadataV2)
      }
      else native.createFresh(inputs.originalMetadata)
      freshService = new MySqlFreshWorldBootstrap(inputs.auditExpectation, auditor, verified, scope,
        native.provisioner, native.markerPublisher, leaseDuration, sharedAllocationLedger.getOrElse(
          throw new IllegalStateException("actual Fresh actor requires the original session allocation ledger")))
      stage = "fresh-readonly-prepare-plan"
      val preparePlan = freshService.planFreshPrepare(native.provisionedWorld, native.anchorStore,
        native.anchorLocation, native.sourceAnchor, native.slotStore, native.nativeIo, held)
      freshLastContext = Some(preparePlan.context()) // originalcontext取得即登记，native失败不能丢弃。
      stage = "fresh-genesis-publication"
      val genesis = freshService.publishPlannedGenesis(preparePlan)
      stage = "fresh-original-prepare-full20-native-witness"
      freshService.publishOriginalPrepareWitness(preparePlan)
      stage = "fresh-planned-prepare-execution"
      acquireAttempted = true // 仅真实W mutation入口之前；GET_LOCK是另一个责任。
      val prepared = freshService.executePlannedPrepare(genesis)
      stage = "fresh-current-marker-publication"
      val marker = freshService.publishPreparedMarker(prepared)
      native.recordMarker(marker.ack())
      stage = "fresh-readonly-certify-plan"
      val certifyPlan = freshService.planFreshCertify(marker)
      freshLastContext = Some(certifyPlan.context())
      stage = "fresh-original-certify-full20-native-witness"
      freshService.publishOriginalCertifyWitness(certifyPlan)
      stage = "fresh-certify-intent-publication"
      val intent = freshService.publishCertifyIntent(certifyPlan)
      stage = "fresh-planned-certify-execution"
      val certified = freshService.executePlannedCertify(intent)
      stage = "fresh-runtime-admission"
      admission = freshService.admitCertifiedRuntime(certified, coordinator)
      MySqlFreshBootRules.requireAdmittedPhase(freshService.plannedCertifyObservation)
      require(coordinator.requireBusinessAdmission(verified, scope) eq admission, "Fresh admission must be the same coordinator reference")
      stage = "fresh-five-trait-repository"
      repository = new MySqlFloatingPodRepository(inputs.auditExpectation, coordinator, verified, scope, sharedAllocationLedger)
      lifecycle.admit()
    catch
      case primary: Throwable =>
        primary match
          case value: MySqlCommitUnknownException => freshLastContext = Some(value.operationContext)
          case value: MySqlBootstrapCommittedUnverifiedException => freshLastContext = value.operationContext.orElse(freshLastContext)
          case value: MySqlBootstrapCleanupUncertainException => freshLastContext = value.operationContext.orElse(freshLastContext)
          case _ => ()
        lifecycle.markFailed(primary) // 原cause先登记，再关闭；不能由cleanup覆盖。
        freshPartialClosed = true
        val observation =
          try performClose(None)
          catch
            case cleanup: Throwable =>
              MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
              throw primary
        MySqlOwnedSessionLifecycle.suppress(primary, new MySqlOwnedSessionInitializationObservation(stage, observation, this))
        throw primary

  /** adapter先登记actual holder，Normal初始化不能绕initial-seed/opaque-native门。 */
  private[mysql] def initializeNormalPreGenesis(): Unit =
    lifecycle.beginOpen()
    require(normalNative.nonEmpty, "explicit Normal pre-genesis owner holder required")
    var stage = "normal-connection-open"
    try
      verified = openRegisteredConnection(new MySqlConnectionSource(inputs.settings))
      stage = "normal-owned-scope"
      scope = MySqlAuditBootScope.forOwnedIo(verified, owner, inputs.bootId, inputs.bootGeneration, connectionGeneration)
      val auditor = inputs.newAuditor()
      coordinator = new MySqlWriterFenceCoordinator(inputs.auditExpectation, auditor, scope, leaseDuration)
      // 当前typed static+READY/noBinding资格在任何nativecreate前；ordinary acquire仍不参与Fresh。
      stage = "normal-current-static-ready-audit"
      val observed = auditor.readCurrentObjectManifest(verified, scope)
      val compared = auditor.compareRequiredObjectManifest(observed)
      val qualification = auditor.auditBeforeWriterAcquire(observed, scope)
      require(compared.actualCanonicalSha256 == qualification.objectManifestSha256 &&
        qualification.physicalBinding == MySqlNoPhysicalBinding, "Normal requires current approved READY/noBinding before native inspection")
      stage = "normal-session-termination-barrier"
      freshBarrierAcquireAttempted = true
      val held = verified.acquireSessionBarrier(inputs.auditExpectation, scope, Duration.ZERO) match
        case MySqlSessionBarrierAcquisition.Held(value) => value
        case MySqlSessionBarrierAcquisition.Deferred =>
          throw new java.sql.SQLTransientConnectionException("Normal source session barrier is busy; deferred before native inspection")
      heldSessionBarrier = Some(held)
      stage = "normal-native-source-inspection"
      val native = normalNative.get
      native.beginInspection(inputs.originalMetadata)
      freshService = new MySqlFreshWorldBootstrap(inputs.auditExpectation, auditor, verified, scope,
        native.provisioner, native.markerPublisher, leaseDuration, sharedAllocationLedger.getOrElse(
          throw new IllegalStateException("actual Fresh actor requires the original session allocation ledger")))
      stage = "normal-readonly-prepare-plan"
      val preparePlan = freshService.planNormalPreGenesis(native.inspection, native.anchorStore,
        native.anchorLocation, native.sourceAnchor, native.slotStore, held)
      freshLastContext = Some(preparePlan.context()) // originalcontext取得即登记，native失败不能丢弃。
      stage = "normal-genesis-publication"
      val genesis = freshService.publishNormalPreGenesis(preparePlan)
      stage = "normal-original-prepare-full20-native-witness"
      freshService.publishOriginalPrepareWitness(genesis.originalPlan())
      native.observeInstalled(freshService.normalPreGenesisObservation.installedWorld.getOrElse(
        throw new IllegalStateException("Actual Normal native installation observation missing")))
      stage = "normal-planned-prepare-execution"
      acquireAttempted = true // 仅真实W mutation入口之前；GET_LOCK是另一个责任。
      val prepared = freshService.executePlannedPrepare(genesis)
      stage = "normal-current-marker-publication"
      val marker = freshService.publishPreparedMarker(prepared)
      native.recordMarker(marker.ack())
      stage = "normal-readonly-certify-plan"
      val certifyPlan = freshService.planFreshCertify(marker)
      freshLastContext = Some(certifyPlan.context())
      stage = "normal-original-certify-full20-native-witness"
      freshService.publishOriginalCertifyWitness(certifyPlan)
      stage = "normal-certify-intent-publication"
      val intent = freshService.publishCertifyIntent(certifyPlan)
      stage = "normal-planned-certify-execution"
      val certified = freshService.executePlannedCertify(intent)
      stage = "normal-runtime-admission"
      admission = freshService.admitCertifiedRuntime(certified, coordinator)
      MySqlFreshBootRules.requireAdmittedPhase(freshService.plannedCertifyObservation)
      require(coordinator.requireBusinessAdmission(verified, scope) eq admission, "Normal admission must be the same coordinator reference")
      stage = "normal-five-trait-repository"
      repository = new MySqlFloatingPodRepository(inputs.auditExpectation, coordinator, verified, scope, sharedAllocationLedger)
      lifecycle.admit()
    catch
      case primary: Throwable =>
        primary match
          case value: MySqlCommitUnknownException => freshLastContext = Some(value.operationContext)
          case value: MySqlBootstrapCommittedUnverifiedException => freshLastContext = value.operationContext.orElse(freshLastContext)
          case value: MySqlBootstrapCleanupUncertainException => freshLastContext = value.operationContext.orElse(freshLastContext)
          case _ => ()
        lifecycle.markFailed(primary) // 原cause先登记，再关闭；不能由cleanup覆盖。
        freshPartialClosed = true
        val observation =
          try performClose(None)
          catch
            case cleanup: Throwable =>
              MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
              throw primary
        MySqlOwnedSessionLifecycle.suppress(primary, new MySqlOwnedSessionInitializationObservation(stage, observation, this))
        throw primary

  private[mysql] def freshBootFacts: MySqlFreshBootFacts =
    lifecycle.requireAdmitted()
    require(freshService != null && admission != null, "actual planned Fresh admission required before publishing facts")
    MySqlFreshBootRules.requireAdmittedPhase(freshService.plannedCertifyObservation)
    require(coordinator.requireBusinessAdmission(verified, scope) eq admission,
      "Fresh facts require same actual coordinator admission")
    val (creation, marker) = freshNative.get.bootFacts
    MySqlFreshBootFacts(creation, marker, inputs.approvedBaseline.identity, inputs.approvedBaseline.fileSha256)

  /** partialclose已经完成：只重放保存观察并关联真实workerfatal，不第二次close/release。 */
  private[mysql] def recordedFreshPartialCloseForExit(workerPrimary: Option[Throwable]): Option[MySqlOwnedSessionCloseObservation] =
    lifecycle.requireOwner()
    require(workerPrimary != null && workerPrimary.forall(_ != null), "actual worker failure option required")
    if !freshPartialClosed then None
    else
      if closeObserved.isEmpty then
        val missing = new IllegalStateException("Fresh partial shutdown lacks its actual recorded observation; responsibility retained")
        val primary = workerPrimary.orElse(lifecycle.failure).getOrElse(missing)
        MySqlOwnedSessionLifecycle.suppress(primary, missing)
        throw primary // 已尝试partialclose但未记录终态，禁止第二次SQL/native close假装修复。
      workerPrimary.foreach { fatal =>
        closeObserved.get.failure.foreach(previous => MySqlOwnedSessionLifecycle.suppress(fatal, previous))
        closeObserved = Some(closeObserved.get.copy(failure = Some(fatal)))
      }
      closeObserved

  private[mysql] def normalBootFacts: MySqlNormalBootFacts =
    lifecycle.requireAdmitted()
    require(normalNative.nonEmpty && freshService != null && admission != null, "actual Normal admission required")
    MySqlFreshBootRules.requireAdmittedPhase(freshService.plannedCertifyObservation)
    require(coordinator.requireBusinessAdmission(verified, scope) eq admission, "same actual Normal coordinator required")
    normalNative.get.facts(inputs.approvedBaseline)

  private def normalResponsibility: Option[MySqlNormalBootstrapResponsibility] =
    normalNative.map(native => MySqlNormalBootstrapResponsibility(native.request, native.observation,
      Option(freshService).map(_.normalPreGenesisObservation), freshLastContext, freshBarrierAcquireAttempted))

  private def freshResponsibility: Option[MySqlFreshBootstrapResponsibility] =
    freshNative.map(native => MySqlFreshBootstrapResponsibility(native.request,
      Option(freshService).map(_.plannedPrepareObservation), Option(freshService).map(_.plannedCertifyObservation),
      freshLastContext, native.observation, freshBarrierAcquireAttempted,
      Option(freshService).exists(_.certifiedWitnessExitComplete)))

  /** 数据库 admission identity，不表示世界/marker/custody/GameReady。 */
  def databaseAdmission: MySqlOwnedDatabaseAdmission =
    lifecycle.requireAdmitted()
    try
      val current = coordinator.requireBusinessAdmission(verified, scope)
      MySqlOwnedDatabaseAdmission(current.bootUuid, inputs.bootGeneration, connectionGeneration,
        current.bindingUuid, current.writerEpoch)
    catch
      case cause: Throwable => lifecycle.markFailed(cause); throw cause

  /** 完整九表原始RR snapshot，不枚举traits推断inventory；仍不授予physical/GameReady。 */
  def readRecoverySnapshot(): MySqlRecoverySnapshot =
    lifecycle.requireAdmitted()
    require(!inRepository, "recovery snapshot cannot run inside repository action")
    try
      val before = coordinator.requireBusinessAdmission(verified, scope)
      require(before eq admission, "snapshot requires exact current admission")
      val snapshot = new MySqlRecoverySnapshotReader(inputs.auditExpectation, inputs.newAuditor(), verified, scope).read()
      val after = coordinator.requireBusinessAdmission(verified, scope)
      require((after eq before) && (after eq admission), "snapshot admission changed across read")
      MySqlOwnedSnapshotAdmissionRules.validate(snapshot,
        MySqlOwnedDatabaseAdmission(after.bootUuid, inputs.bootGeneration, connectionGeneration, after.bindingUuid, after.writerEpoch),
        after.token.leaseUntil, inputs.auditExpectation.expectedPhysicalFormat.version)
      snapshot
    catch
      case cause: Throwable => lifecycle.markFailed(cause); throw cause

  // Session 在解码/公开投影之前即保留实际 raw source；失败不会只留下 budget 数字。
  private var originalBootReadSource: MySqlFormat3FullSnapshot = null
  private var originalBootReadProjection: MySqlOwnedFormat3Snapshot = null
  private var freshCompletionPrepared = false
  private var freshBootRootClosed = false
  private var freshNativeReleaseEligible = false
  private[mysql] def retainCompletionExitFailure(cause: Throwable): Unit =
    lifecycle.requireOwner()
    require(cause != null, "actual completion exit failure required")
    lifecycle.markFailed(cause)
  private[mysql] def prepareCertifiedFreshCompletion(handoff: MySqlOwnedIoAdapter.FreshCompletionHandoff): Unit =
    lifecycle.requireAdmitted()
    require(!inRepository && !freshCompletionPrepared && handoff != null && freshNative.nonEmpty && freshService != null &&
      originalBootReadSource != null && (handoff.snapshot eq originalBootReadProjection),
      "original Fresh Main handoff and owned Boot source required")
    try
      retainedPlanIssuer.getOrElse(throw new IllegalStateException("original Fresh retained-plan issuer missing"))
        .requireFreshCompletionSeal()
      require(originalBootReadProjection.requireOriginalBootEpoch(handoff.tag) == handoff.epoch,
        "Fresh completion handoff differs from original accepted Boot work")
      freshService.prepareCertifiedCompletionExit(coordinator, admission)
      val root = originalBootReadProjection.detachOriginalBootInputForExit(originalBootReadSource, handoff.tag)
      originalBootReadProjection = null
      originalBootReadSource = null
      root.closeRootReadBudget()
      freshBootRootClosed = true
      freshLastContext = None
      freshService.dropCertifiedWitnessesForExit()
      freshCompletionPrepared = true
    catch
      case cause: Throwable => lifecycle.markFailed(cause); throw cause
  /** 格式3 Boot 一次完整二十关系只读投影；不退回九表恢复视图。 */
  def readFullFormat3Snapshot(actualBootTag: IoOperationTag, capturedRepositoryEpoch: Long): MySqlOwnedFormat3Snapshot =
    lifecycle.requireAdmitted()
    require(!inRepository, "format3 full snapshot cannot run inside repository action")
    try
      require(originalBootReadSource == null, "owned Boot full20 read source is retained once")
      require(actualBootTag != null && actualBootTag.bootId == inputs.bootId &&
        actualBootTag.bootGeneration == inputs.bootGeneration && actualBootTag.connectionGeneration == connectionGeneration &&
        capturedRepositoryEpoch >= 0L, "original accepted Boot work identity/epoch required")
      val before = coordinator.requireBusinessAdmission(verified, scope)
      require(before eq admission, "format3 full snapshot requires exact current admission")
      val snapshot = new MySqlFormat3FullSnapshotReader(verified, scope,
        inputs.auditExpectation.schema, inputs.format3Contract.limitProfile, sharedAllocationLedger).read()
      originalBootReadSource = snapshot // 原 session 已先发布给 actual adapter；无新 holder allocation。
      require(snapshot.observationScope.bootUuid == inputs.bootId &&
        snapshot.observationScope.bootGeneration == inputs.bootGeneration &&
        snapshot.observationScope.connectionGeneration == connectionGeneration,
        "format3 full snapshot was captured for another owned generation")
      require(snapshot.connection.serverUuid == inputs.approvedBaseline.identity.serverUuid &&
        snapshot.connection.serverVersion == inputs.approvedBaseline.identity.serverVersion &&
        snapshot.schema == inputs.approvedBaseline.identity.schema,
        "format3 full snapshot belongs to another approved server/schema")
      val after = coordinator.requireBusinessAdmission(verified, scope)
      require((after eq before) && (after eq admission), "format3 snapshot admission changed across read")
      originalBootReadProjection = MySqlOwnedFormat3Snapshot.fromAcceptedBoot(snapshot, actualBootTag, capturedRepositoryEpoch)
      originalBootReadProjection
    catch
      case cause: Throwable => lifecycle.markFailed(cause); throw cause

  /** 原事实槽先接管新结果；104只收新backing和wrapper，闭后仍保原账。 */
  private[mysql] def issueOrdinaryBootResult(tag: IoOperationTag, epoch: Long,
      inventory: MySqlOwnedFormat3Snapshot, exactFacts: MySqlOrdinaryFacts): MySqlOwnedIoReply.OrdinaryCertifiedBooted =
    lifecycle.requireAdmitted()
    require(!inRepository && originalBootReadSource != null && (inventory eq originalBootReadProjection),
      "exact original Ordinary Boot projection required")
    ordinaryFactsValue match
      case facts: MySqlOrdinaryFacts => require(facts eq exactFacts, "original Ordinary facts slot differs")
      case _ => throw new IllegalStateException("Ordinary result is already issued or unavailable")
    require(inventory.requireOriginalBootEpoch(tag) == epoch, "original Boot tag/epoch required")
    val budget = originalBootReadSource.allocationBudget
    require(!budget.isClosed && (budget.profile eq inputs.format3Contract.limitProfile) &&
      sharedAllocationLedger.nonEmpty, "actual live Boot root/shared ledger required")
    budget.requireLedgerIdentity(sharedAllocationLedger.get)
    try
      budget.reserveRetainedMemory(104L, "Ordinary application result backing80/wrapper24; retained after application drop")
      val backing = new MySqlOwnedIoAdapter.OrdinaryBootResultBacking(databaseAdmission, inventory, exactFacts,
        inputs.format3Contract.limitProfile)
      ordinaryFactsValue = backing
      try
        val result = new MySqlOwnedIoReply.OrdinaryCertifiedBooted(backing)
        backing.publish()
        result
      catch
        case cause: Throwable =>
          backing.retainFailure(cause)
          throw cause
    catch
      case cause: Throwable =>
        lifecycle.markFailed(cause)
        throw cause

  /** 只读原事实阶段槽；Main仅核同对象，不取得预算或worker权限。 */
  private[mysql] def requireOriginalOrdinaryBootBacking(expected: MySqlOwnedIoAdapter.OrdinaryBootResultBacking): Unit =
    ordinaryFactsValue match
      case actual: MySqlOwnedIoAdapter.OrdinaryBootResultBacking => require(actual eq expected, "original Session application result differs")
      case _ => throw new IllegalStateException("original Session application result is unavailable")

  private var retainedPlanIssuer = Option.empty[MySqlOwnedRetainedPlanIssuer]

  /** 仅实际 admitted worker 成功Boot inventory/freshness后发行一次共享纯内存issuer。 */
  private[mysql] def createRetainedPlanIssuer(identity: MySqlOwnedIoIdentity,
      actualMain: Thread, capacity: Int): MySqlOwnedRetainedPlanIssuer =
    lifecycle.requireAdmitted()
    require(!inRepository && retainedPlanIssuer.isEmpty && identity != null && actualMain != null &&
      identity.bootId == inputs.bootId && identity.bootGeneration == inputs.bootGeneration &&
      identity.connectionGeneration == connectionGeneration, "one exact actual session issuer required")
    val held = heldSessionBarrier.getOrElse(
      throw new IllegalStateException("retained plan issuer lacks actual held barrier"))
    held.readmit(verified, inputs.auditExpectation, scope)
    require(coordinator.requireBusinessAdmission(verified, scope) eq admission,
      "retained plan issuer requires exact current business admission")
    val issuer = new MySqlOwnedRetainedPlanIssuer(this, identity, owner, actualMain,
      inputs.format3Contract.limitProfile, sharedAllocationLedger.getOrElse(
        throw new IllegalStateException("actual format3 shared ledger required")), capacity)
    retainedPlanIssuer = Some(issuer)
    issuer

  /** 只投影该实际已装配 session 的不可变 profile 数据；不发行 writer/SQL/Ready 权限。 */
  def actualSourceLimitProfile: SourceLimitProfile =
    lifecycle.requireAdmitted()
    require(!inRepository, "profile projection cannot escape a repository borrow")
    inputs.format3Contract.limitProfile

  /** 只借具体限时 five-trait view；操作 Failure 立即 sticky，即使 callback 吞掉后返回 Success。 */
  def useRepository[A](body: MySqlOwnedRepositoryView => Try[A]): Try[A] =
    useRepositoryScoped(None, None, body)

  /** 原 command 的精确 operation ID 随同 borrow；不能在 repository 内重造重试身份。 */
  def useRepository[A](tag: IoOperationTag)(body: MySqlOwnedRepositoryView => Try[A]): Try[A] =
    useRepositoryScoped(Some(tag), None, body)

  /** Main 原 plan 必须在首次 scope 发行时绑定，不能升级已发行的 tag-only scope。 */
  def useRepository[A](tag: IoOperationTag, plan: MySqlOwnedRepositoryOperationPlan)
      (body: MySqlOwnedRepositoryView => Try[A]): Try[A] =
    useRepositoryScoped(Some(tag), Some(plan), body)

  /** Consume an immutable Main memory-release ACK on the actual worker; this never releases SQL or Forge resources. */
  private[mysql] def releaseModeProjection(tag: IoOperationTag, receipt: MySqlRetainedPlanDropReceipt): Unit =
    lifecycle.requireAdmitted()
    require(!inRepository, "mode projection ACK cannot nest inside a repository borrow")
    require(tag != null && tag.bootId == inputs.bootId && tag.bootGeneration == inputs.bootGeneration &&
      tag.connectionGeneration == connectionGeneration, "mode projection ACK differs from the exact owned session generation")
    val held = heldSessionBarrier.getOrElse(
      throw new IllegalStateException("mode projection ACK requires the actual retained session barrier"))
    held.readmit(verified, inputs.auditExpectation, scope)
    require(coordinator.requireBusinessAdmission(verified, scope) eq admission,
      "mode projection ACK requires the exact current owned session admission")
    val issuer = exactDroppedIssuer(tag, receipt)
    issuer.requireReleaseKind(receipt, MySqlRetainedPlanReleaseKind.ModeProjection)
    issuer.dropWorkerPlanReferences(receipt)
    try
      repository.releaseRetainedModeProjection(tag)
      issuer.releaseDropped(receipt)
    catch
      case cause: Throwable =>
        issuer.markReleaseFailed(receipt, cause)
        lifecycle.markFailed(cause)
        throw cause

  private[mysql] def releaseDriftProjection(tag: IoOperationTag, receipt: MySqlRetainedPlanDropReceipt): Unit =
    lifecycle.requireAdmitted()
    require(!inRepository, "mode projection ACK cannot nest inside a repository borrow")
    require(tag != null && tag.bootId == inputs.bootId && tag.bootGeneration == inputs.bootGeneration &&
      tag.connectionGeneration == connectionGeneration, "mode projection ACK differs from the exact owned session generation")
    val held = heldSessionBarrier.getOrElse(
      throw new IllegalStateException("mode projection ACK requires the actual retained session barrier"))
    held.readmit(verified, inputs.auditExpectation, scope)
    require(coordinator.requireBusinessAdmission(verified, scope) eq admission,
      "mode projection ACK requires the exact current owned session admission")
    val issuer = exactDroppedIssuer(tag, receipt)
    issuer.requireReleaseKind(receipt, MySqlRetainedPlanReleaseKind.DriftProjection)
    issuer.dropWorkerPlanReferences(receipt)
    try
      repository.releaseRetainedDriftProjection(tag)
      issuer.releaseDropped(receipt)
    catch
      case cause: Throwable =>
        issuer.markReleaseFailed(receipt, cause)
        lifecycle.markFailed(cause)
        throw cause

  /** ordinary canonical计划结算不新mint SQL操作；仅当前worker/admission/原issuer内存ACK。 */
  private[mysql] def releaseRetainedPlan(tag: IoOperationTag, receipt: MySqlRetainedPlanDropReceipt): Unit =
    lifecycle.requireAdmitted()
    require(!inRepository, "retained plan release cannot nest in a repository borrow")
    require(coordinator.requireBusinessAdmission(verified, scope) eq admission,
      "retained plan release requires exact current actual admission")
    val issuer = exactDroppedIssuer(tag, receipt)
    issuer.requireReleaseKind(receipt, MySqlRetainedPlanReleaseKind.Ordinary)
    try
      if repository.hasRetainedNormalRecallProjection(tag) then
        repository.requireNormalRecallProjectionRelease(tag)
        issuer.dropWorkerPlanReferences(receipt)
        repository.releaseRetainedNormalRecallProjection(tag)
      else if repository.hasRetainedControlledDebugProjection(tag) then
        issuer.dropWorkerPlanReferences(receipt)
        repository.releaseRetainedControlledDebugProjection(tag)
      else if repository.hasRetainedInactiveDomainProjection(tag) then
        issuer.dropWorkerPlanReferences(receipt)
        repository.releaseRetainedInactiveDomainProjection(tag)
      issuer.releaseDropped(receipt)
    catch
      case cause: Throwable =>
        try issuer.markReleaseFailed(receipt,cause)
        catch case secondary: Throwable => MySqlOwnedSessionLifecycle.suppress(cause,secondary)
        try lifecycle.markFailed(cause)
        catch case secondary: Throwable => MySqlOwnedSessionLifecycle.suppress(cause,secondary)
        throw cause

  private def exactDroppedIssuer(tag: IoOperationTag,
      receipt: MySqlRetainedPlanDropReceipt): MySqlOwnedRetainedPlanIssuer =
    require(receipt != null && tag == receipt.tag, "exact original Main drop receipt required")
    val issuer = retainedPlanIssuer.getOrElse(throw new IllegalStateException("actual retained issuer missing"))
    require(receipt.handle.issuer eq issuer, "foreign session drop receipt")
    issuer.requireSession(this, inputs.format3Contract.limitProfile, sharedAllocationLedger.getOrElse(
      throw new IllegalStateException("actual retained root ledger missing")))
    issuer.requireDropReceipt(receipt)
    issuer

  /** 原 Main plan 的全只读 vector；capture 先于 borrow，绝不绕过公开 inRepository guard。 */
  private[mysql] def executeQueries(tag: IoOperationTag, plan: MySqlOwnedRepositoryOperationPlan,
      operations: Vector[MySqlRepositoryOperation]): Vector[MySqlRepositoryReply] =
    lifecycle.requireAdmitted()
    require(!inRepository,"query cannot nest in repository action")
    var borrow: MySqlOwnedRepositoryBorrow = null
    try
      MySqlFormat3ReadDispatcher.requireReads(operations)
      require(tag != null && plan != null && tag.bootId == inputs.bootId && tag.bootGeneration == inputs.bootGeneration &&
        tag.connectionGeneration == connectionGeneration,"query requires exact owned tag/generation")
      plan.requireTag(tag)
      plan.requestKind match
        case "RUN" if operations.size == 1 => plan.requireSingle(operations.head,inputs.format3Contract.limitProfile)
        case "TRANSACTION" => plan.requireTransaction(operations,inputs.format3Contract.limitProfile)
        case _ => throw new IllegalArgumentException("query vector differs from original plan kind")
      require(plan.exactLimitProfile eq inputs.format3Contract.limitProfile,"query requires actual profile reference")
      val held = heldSessionBarrier.getOrElse(throw new IllegalStateException("query lacks actual held session barrier"))
      held.readmit(verified,inputs.auditExpectation,scope)
      val before = coordinator.requireBusinessAdmission(verified,scope)
      require(before eq admission,"query requires actual current admission")
      val handle = plan.retainedHandle.getOrElse(throw new IllegalArgumentException("query lacks original reservation"))
      handle.issuer.requireSession(this,inputs.format3Contract.limitProfile,sharedAllocationLedger.getOrElse(
        throw new IllegalStateException("query lacks actual shared allocation ledger")))
      val root = handle.issuer.beginExecution(plan)
      val reader = new MySqlFormat3FullSnapshotReader(verified,scope,inputs.auditExpectation.schema,
        inputs.format3Contract.limitProfile,sharedAllocationLedger)
      val captured = MySqlFormat3ReadDispatcher.capture(operations,reader,root)
      require(captured.allocationBudget eq root,"query full20 reader lost original reserved root")
      MySqlFormat3QueryProjection.requireAdmittedSnapshot(captured,inputs.auditExpectation,before)
      held.readmit(verified,inputs.auditExpectation,scope)
      require(heldSessionBarrier.exists(_ eq held) && (coordinator.requireBusinessAdmission(verified,scope) eq before) &&
        (admission eq before),"query admission/barrier changed across capture")
      val projection = MySqlFormat3QueryProjection.from(captured,root)
      inRepository = true
      borrowGeneration = Math.incrementExact(borrowGeneration)
      borrow = new MySqlOwnedRepositoryBorrow(owner,borrowGeneration,() => activeBorrow.exists(_ eq borrow),
        () => lifecycle.failure,cause => lifecycle.markFailed(cause))
      activeBorrow = Some(borrow)
      val view = new MySqlOwnedRepositoryView(repository,borrow,Some(projection))
      root.reserveRetainedMemory(Math.addExact(128L,Math.multiplyExact(operations.size.toLong,64L)),"query reply vector")
      val replies = borrow.complete(Try(MySqlFormat3ReadDispatcher.executeOwned(operations,view))).get
      borrow.throwIfFailed()
      held.readmit(verified,inputs.auditExpectation,scope)
      require(heldSessionBarrier.exists(_ eq held) && (coordinator.requireBusinessAdmission(verified,scope) eq before) &&
        (admission eq before),"query admission changed before completion")
      replies
    catch
      case cause: Throwable => lifecycle.markFailed(cause); throw cause
    finally
      if borrow != null then borrow.revoke()
      activeBorrow = None
      inRepository = false

  private def useRepositoryScoped[A](commandTag: Option[IoOperationTag],
                                    commandPlan: Option[MySqlOwnedRepositoryOperationPlan],
                                    body: MySqlOwnedRepositoryView => Try[A]): Try[A] =
    lifecycle.requireAdmitted()
    require(body != null && !inRepository, "owned repository action must be non-null and non-nested")
    inRepository = true
    var borrow: MySqlOwnedRepositoryBorrow = null
    try
      commandTag.foreach { tag =>
        require(tag != null && tag.bootId == inputs.bootId && tag.bootGeneration == inputs.bootGeneration &&
          tag.connectionGeneration == connectionGeneration, "repository tag differs from exact owned database session")
      }
      commandPlan.foreach { plan =>
        require(plan != null && commandTag.nonEmpty, "repository plan requires its original command tag")
        plan.requireTag(commandTag.get)
        require(plan.exactLimitProfile == inputs.format3Contract.limitProfile,
          "repository plan differs from the actual approved session limit profile")
      }
      val held = heldSessionBarrier.getOrElse(
        throw new IllegalStateException("owned repository action lacks its actual retained session barrier"))
      held.readmit(verified, inputs.auditExpectation, scope)
      require(coordinator.requireBusinessAdmission(verified, scope) eq admission,
        "repository action requires exact current session admission")
      borrowGeneration = Math.incrementExact(borrowGeneration)
      borrow = new MySqlOwnedRepositoryBorrow(owner, borrowGeneration,
        () => activeBorrow.exists(_ eq borrow),
        () => lifecycle.failure,
        cause => if closeObserved.isEmpty then lifecycle.markFailed(cause), commandTag, commandTag.map(_ => scope),
        commandTag.map(_ => held), commandTag.map(_ => admission))
      activeBorrow = Some(borrow)
      val view = new MySqlOwnedRepositoryView(repository, borrow)
      commandPlan match
        case Some(plan) =>
          val handle = plan.retainedHandle.getOrElse(
            throw new IllegalArgumentException("planned business command lacks its actual session reservation"))
          handle.issuer.requireSession(this, inputs.format3Contract.limitProfile, sharedAllocationLedger.getOrElse(
            throw new IllegalStateException("planned business command lacks actual format3 root ledger")))
          val originalRoot = handle.issuer.beginExecution(plan)
          MySqlRecoveryCanonicalBytes.withAllocationBudget(originalRoot) {
            view.withCommandScope(plan)(_ => borrow.complete(body(view)))
          }
        case None if commandTag.nonEmpty => view.withCommandScope(_ => borrow.complete(body(view)))
        case None => borrow.complete(body(view))
    catch
      case cause: Throwable =>
        lifecycle.markFailed(cause)
        throw cause
    finally
      if borrow != null then borrow.revoke()
      activeBorrow = None
      inRepository = false

  def renewLease(): MySqlOwnedDatabaseAdmission =
    lifecycle.requireAdmitted()
    require(!inRepository, "renewal cannot run inside repository action")
    try
      val current = coordinator.requireBusinessAdmission(verified, scope)
      require(current eq admission, "renewal requires exact current session admission reference")
      val nextSequence = Math.incrementExact(renewSequence)
      val token = coordinator.renew(current, verified, scope)
      admission = coordinator.auditAndConfirmPostAcquire(token)
      renewSequence = nextSequence
      databaseAdmission
    catch
      case cause: Throwable => lifecycle.markFailed(cause); throw cause

  /** 已核 current admission 的只读窗口；绝不把旧 identity DTO 当作期限证明。 */
  private[mysql] def isRecallRecovering: Boolean =
    lifecycle.requireOwner()
    lifecycle.failure.foreach(throw _)
    recallRecovery.nonEmpty

  private def requireRecallRecovery(): MySqlCertifiedOrdinaryCapabilities.RecallRecoveryAdmission =
    lifecycle.requireRecallRecovering()
    require(!inRepository, "restricted Recall cannot enter a business repository borrow")
    val original = recallRecovery.getOrElse(throw new IllegalStateException("original Recall issuer missing"))
    coordinator.requireRecallRecovery(original)
    original

  private[mysql] def currentRecallFreshness(): MySqlOwnedAdmissionFreshness =
    try
      val value = requireRecallRecovery().receipt.ownedFreshness(renewSequence)
      require(value.isCurrent, "original restricted Recall observation expired")
      value
    catch
      case cause: Throwable => lifecycle.markFailed(cause); throw cause

  private[mysql] def maintainRecallRecovery(): MySqlOwnedAdmissionFreshness =
    try
      val original = requireRecallRecovery()
      val next = Math.incrementExact(renewSequence)
      original.actor.maintainRecallRecovery(original, coordinator)
      renewSequence = next
      currentRecallFreshness()
    catch
      case cause: Throwable => lifecycle.markFailed(cause); throw cause

  /** 完整实际locked inventory仍由原Actor/root持有；本投影没有business admission。 */
  private[mysql] def readRecallBootSnapshot(tag: IoOperationTag, epoch: Long): MySqlOwnedFormat3Snapshot =
    val original = requireRecallRecovery()
    require(originalBootReadSource == null && originalBootReadProjection == null && tag != null &&
      tag.bootId == inputs.bootId && tag.bootGeneration == inputs.bootGeneration &&
      tag.connectionGeneration == connectionGeneration && epoch >= 0L, "exact one-shot restricted Boot input required")
    originalBootReadSource = original.snapshot
    originalBootReadProjection = MySqlOwnedFormat3Snapshot.fromAcceptedBoot(original.snapshot, tag, epoch)
    originalBootReadProjection

  private[mysql] def recallReferences: Vector[MySqlNormalRecallRecoveryReference] =
    val original = requireRecallRecovery()
    original.snapshot.allocationBudget.reserveRetainedMemory(
      Math.multiplyExact(256L, original.inventory.size.toLong), "restricted Main recall references")
    original.inventory.map(i => MySqlNormalRecallRecoveryReference(i.workflow, i.pod, i.prepare, i.checkpoint,
      original.checkpointObservation(i)))

  private[mysql] def currentFreshness(): MySqlOwnedAdmissionFreshness =
    lifecycle.requireOwner()
    lifecycle.failure.foreach(cause => throw cause)
    lifecycle.requireAdmitted()
    require(!inRepository, "freshness cannot be projected inside repository action")
    try
      val current = coordinator.requireBusinessAdmission(verified, scope)
      require(current eq admission, "freshness requires exact current coordinator admission")
      val value = current.receipt.ownedFreshness(renewSequence)
      require(value.isCurrent, "actual admission projection expired before publication")
      value
    catch
      case cause: Throwable => lifecycle.markFailed(cause); throw cause

  /** 同一实际 owned connection、scope、coordinator renewal +完整 post-audit；无 acquire/reconnect。 */
  private[mysql] def maintainAdmission(): MySqlOwnedAdmissionFreshness =
    lifecycle.requireOwner()
    lifecycle.failure.foreach(cause => throw cause)
    lifecycle.requireAdmitted()
    require(!inRepository, "maintenance cannot run inside repository action")
    renewLease() // requireBusinessAdmission 拒绝 expired/Unknown/frozen；原失败立即 sticky。
    currentFreshness()

  /** 外部 owned-work failure 也必须在该 worker 登记，不能以看似健康的 coordinator 擦除责任。 */
  def markFailed(cause: Throwable): Unit =
    lifecycle.requireOwner()
    require(cause != null, "original owned work failure required")
    if freshPartialClosed && closeObserved.nonEmpty then
      val existing = closeObserved.get
      closeObserved = Some(existing.copy(failure = MySqlOwnedSessionLifecycle.combine(existing.failure, cause)))
    else lifecycle.markFailed(cause)

  def closeObservation: Option[MySqlOwnedSessionCloseObservation] =
    lifecycle.requireOwner()
    closeObserved

  /** 必须由 actual same-worker exit hook 传入该 IO.checkpoint.fatalCause，不接受人工 None 冒充健康。
    * 记录观察后若有 primary/unresolved failure 则原样抛出供 HookFailed；不制造 IOExitProof。
    */
  def closeForWorkerExit(primaryWorkerFailure: Option[Throwable]): MySqlOwnedSessionCloseObservation =
    lifecycle.requireOwner()
    require(!inRepository, "worker shutdown cannot run inside repository action")
    val observation = performClose(primaryWorkerFailure)
    observation.failure match
      case Some(cause) => throw cause
      case None => observation

  /** 原hook即使预核失败仍进入同一次performClose，避免跳过原JDBC清理。 */
  private[mysql] def closeOrdinaryForWorkerExit(primaryWorkerFailure: Option[Throwable],
      receipt: com.wi.floatingpod.network.DedicatedProjectionRuntime.MainConsumersEnded,
      originalBootTag: IoOperationTag): MySqlOwnedSessionCloseObservation =
    lifecycle.requireOwner()
    if inRepository then throw new IllegalStateException("ordinary exit cannot run inside repository action")
    val observation=performClose(primaryWorkerFailure,receipt,originalBootTag)
    if observation.failure.nonEmpty then throw observation.failure.get
    observation

  /** 只在真实Fresh综合路径与资源close均完成后撤原issuer；调用方短锁不包含资源操作。 */
  private[mysql] def detachUnusedFreshIssuerForExit(expected: MySqlOwnedRetainedPlanIssuer): Unit =
    lifecycle.requireOwner()
    require(v2Lease != null && expected != null, "exact V2 Fresh issuer custody required")
    require(freshCompletionPrepared && freshBootRootClosed && freshNative.nonEmpty && freshService != null &&
      freshService.certifiedWitnessExitComplete, "actual Fresh completion preparation is not settled")
    val observation = closeObserved.getOrElse(throw new IllegalStateException("actual Fresh Session close observation required"))
    require(!observation.unresolvedOwnedResponsibility && activeBorrow.isEmpty && !inRepository &&
      originalBootReadSource == null && originalBootReadProjection == null,
      "Fresh Session still retains original resource/Boot responsibility")
    require(retainedPlanIssuer.exists(_ eq expected), "actual Session issuer differs before Fresh retirement")
    expected.detachUnusedFreshReferencesForExit(this, inputs.format3Contract.limitProfile,
      sharedAllocationLedger.getOrElse(throw new IllegalStateException("actual Fresh shared ledger required")))
    retainedPlanIssuer = None

  private[mysql] def detachOrdinaryIssuerForExit(expected: MySqlOwnedRetainedPlanIssuer): Unit =
    lifecycle.requireOwner()
    if closeObserved.isEmpty then throw new IllegalStateException("actual Ordinary close observation required")
    val observation=closeObserved.get
    if observation.failure.nonEmpty then throw observation.failure.get
    if observation.unresolvedOwnedResponsibility || activeBorrow.nonEmpty || inRepository ||
        originalBootReadSource != null || originalBootReadProjection != null ||
        (retainedPlanIssuer.isEmpty || (retainedPlanIssuer.get ne expected)) then
      throw new IllegalStateException("actual Ordinary Session still retains terminal resources")
    issuedFreshNativeExit match
      case value: MySqlCertifiedOrdinaryNativeExitPermit => value.requireDetachedShell()
      case _ => throw new IllegalStateException("actual Ordinary native terminal shell required")
    if sharedAllocationLedger.isEmpty then throw new IllegalStateException("original shared allocation ledger required")
    expected.detachOrdinaryReferencesForExit(this,inputs.format3Contract.limitProfile,sharedAllocationLedger.get)
    retainedPlanIssuer=None

  private var v2GraphDropped=false
  private[mysql] def retireV2GraphForExit(): Unit =
    lifecycle.requireOwner()
    require(v2Lease != null,"explicit V2 Session custody required")
    if v2GraphDropped then return
    val observation=closeObserved.getOrElse(throw new IllegalStateException("Session close observation required before graph retirement"))
    require(!observation.unresolvedOwnedResponsibility && activeBorrow.isEmpty && !inRepository,
      "Session still retains SQL/native/repository responsibility")
    require(originalBootReadSource == null && originalBootReadProjection == null,"original Boot image still retains runtime inputs")
    // Main已取得的issuer仍保exactSession/profile；空entries不是consumer终结证明。
    require(retainedPlanIssuer.isEmpty,"initialized Main issuer consumer termination is not yet proven; retain V2 custody")
    recoveryActor.foreach(_.dropRuntimeGraphForExit())
    require(freshService == null || freshService.certifiedWitnessExitComplete,"Fresh service still retains witness/runtime graph")
    freshNative.foreach(_.dropRuntimeConfigurationForExit())
    // 报告已先保存；只撤销已退出资源的静态引用，不用清字段替代实际资源结算。
    repository=null;coordinator=null;verified=null;scope=null;admission=null;heldSessionBarrier=None
    freshService=null;freshLastContext=None;retainedPlanIssuer=None
    ordinaryCompletion=None;ordinaryFactsValue=null;recallRecovery=None
    v2Lease.close()
    v2GraphDropped=true

  private def performClose(workerPrimary: Option[Throwable],
      ordinaryReceipt: com.wi.floatingpod.network.DedicatedProjectionRuntime.MainConsumersEnded = null,
      ordinaryBootTag: IoOperationTag = null): MySqlOwnedSessionCloseObservation =
    // Main实际未结算引用通过原issuer保留；worker在既有close边界消费原cause，不用queue-empty假release。
    retainedPlanIssuer.flatMap(_.observation.failure).foreach(lifecycle.markFailed)
    lifecycle.beginClose(workerPrimary)
    var ordinaryState: MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement = null
    if ordinaryReceipt != null && lifecycle.failure.isEmpty then
      try
        ordinaryReceipt.requireRegisteredIdentity(inputs.bootId,inputs.bootGeneration,connectionGeneration)
        if ordinaryBootTag == null || !ordinaryCompletionClaimed || ordinaryCompletion.isEmpty ||
            originalBootReadSource == null || originalBootReadProjection == null then
          throw new IllegalStateException("actual claimed Ordinary completion and retained Boot input required")
        ordinaryFactsValue match
          case value: MySqlOwnedIoAdapter.OrdinaryBootResultBacking => value.requireRetired()
          case _ => throw new IllegalStateException("actual application Boot result retirement required")
        if retainedPlanIssuer.isEmpty then throw new IllegalStateException("original Ordinary issuer required")
        retainedPlanIssuer.get.requireOrdinaryTerminalPrepared()
        if recoveryActor.isEmpty then throw new IllegalStateException("registered Ordinary actor required")
        val actor=recoveryActor.get
        ordinaryState=actor.prepareOrdinaryExit() // actor原result先挂40B状态，后续失败仍可达。
        val permit=actor.ordinaryNativeExitPermit(ordinaryState)
        issuedFreshNativeExit=permit // 正常返回先挂原单槽；其间无fallible操作或Some。
      catch case cause: Throwable => lifecycle.markFailed(cause)
    var release = if acquireAttempted then MySqlOwnedSessionRelease.RetainedNotAdmitted else MySqlOwnedSessionRelease.NoAcquireAttempt
    var releaseAttempted = false
    var releaseReturned = false
    var jdbcCloseAttempted = false
    var jdbcClosed = Option.empty[Boolean]
    var sessionBarrier = Option.empty[MySqlSessionBarrierCloseObservation]
    var frozen = if coordinator == null then None else coordinator.frozenFailure
    if lifecycle.failure.nonEmpty || frozen.nonEmpty then
      if acquireAttempted then release = MySqlOwnedSessionRelease.RetainedFailure
      frozen.foreach(lifecycle.markFailed)
    else if lifecycle.releaseEligible && coordinator != null && admission != null then
      try
        // 无任何 release shortcut：每次重新核 exact current admission、token，再由 coordinator fresh audit/DB-time/CAS。
        val current = coordinator.requireBusinessAdmission(verified, scope)
        require(current eq admission, "owned shutdown admission identity changed")
        releaseAttempted = true
        coordinator.release(current.token, verified, scope)
        releaseReturned = true
        release = MySqlOwnedSessionRelease.Released
      catch
        case cause: Throwable =>
          lifecycle.markFailed(cause)
          release =
            if releaseAttempted then MySqlOwnedSessionRelease.ReleaseFailed(cause)
            else MySqlOwnedSessionRelease.RetainedAdmissionUnavailable(cause)
    if verified != null then
      jdbcCloseAttempted = true
      try
        if repository != null then repository.close()
        else verified.close()
      catch case cleanup: Throwable => lifecycle.markFailed(cleanup)
      // repository 内部 closed flag 与 close() 返回均不是实际 JDBC liveness；即使 close 抛异常也尝试 getter。
      try
        jdbcClosed = Some(verified.connection.isClosed)
        if !jdbcClosed.contains(true) then
          lifecycle.markFailed(new IllegalStateException("owned JDBC isClosed observed false after close attempt"))
      catch case observationFailure: Throwable => lifecycle.markFailed(observationFailure)
    if verified == null && openHolder != null && openHolder.hasRawHandle() then
      jdbcCloseAttempted = openHolder.closeAttempted()
      try
        jdbcClosed = Some(openHolder.rawConnectionClosed())
        if !jdbcClosed.contains(true) then
          lifecycle.markFailed(new IllegalStateException("owned pre-handoff JDBC isClosed observed false after close attempt"))
      catch case observationFailure: Throwable => lifecycle.markFailed(observationFailure)
    if verified != null then
      try sessionBarrier = Some(verified.sessionBarrierCloseObservation())
      catch case observationFailure: Throwable => lifecycle.markFailed(observationFailure)
    // JDBC close/isClosed 只作本地观察，不消除 Held 或 GET_LOCK reply-unknown 的服务端责任。
    freshNative.foreach { native =>
      try
        if freshCompletionPrepared && lifecycle.failure.isEmpty && releaseReturned && jdbcClosed.contains(true) &&
            sessionBarrier.exists(value => !value.mayRetainServerLock) && freshBootRootClosed &&
            freshService.certifiedWitnessExitComplete then
          freshNativeReleaseEligible = true
          val permit=MySqlCertifiedFreshExitPermit.issue(this, native.provisionedWorld, native.nativeIo)
          issuedFreshNativeExit = permit
          native.closeCertifiedPreparation(permit)
        else native.settleForExit()
      catch case cleanup: Throwable => lifecycle.markFailed(cleanup)
    }
    normalNative.foreach { native =>
      try native.settleForExit()
      catch case cleanup: Throwable => lifecycle.markFailed(cleanup)
    }
    recoveryActor match
      case Some(actor) =>
        try
          if ordinaryReceipt != null && ordinaryState != null && lifecycle.failure.isEmpty && releaseReturned &&
              jdbcClosed.contains(true) && (sessionBarrier.nonEmpty && !sessionBarrier.get.mayRetainServerLock) then
            val permit=issuedFreshNativeExit match
              case value: MySqlCertifiedOrdinaryNativeExitPermit => value
              case _ => throw new IllegalStateException("original prepared Ordinary native permit missing")
            permit.sqlSettlementReturned(this)
            // 原SQL末consumer已返回；先撤Session回边，再结束Actor/native消费者。
            ordinaryCompletion=None;recallRecovery=None;ordinaryFactsValue=null
            actor.detachOrdinaryBusinessConsumers(ordinaryState)
            actor.closeOrdinaryNativeAndBorrowGroup(ordinaryState,permit)
            val actorRoot=actor.originalOrdinaryExitRoot(ordinaryState)
            val bootRoot=originalBootReadProjection.detachOriginalBootInputForExit(originalBootReadSource,ordinaryBootTag)
            if bootRoot ne actorRoot then
              bootRoot.endCertifiedOrdinaryBootBorrowGroup(this,permit)
              bootRoot.closeRootReadBudget()
              originalBootReadSource=null;originalBootReadProjection=null
            actorRoot.closeRootReadBudget()
            actor.ordinaryRootCloseReturned(ordinaryState)
            if bootRoot eq actorRoot then
              originalBootReadSource=null;originalBootReadProjection=null
            permit.detachAfterRootClose(this)
            inspectionResult=Some(MySqlCertifiedInspectionObservation("ORDINARY_NATIVE_AND_ROOT_CLOSED",
              None,None,None,false,false,None))
          else actor.settleForExit()
        catch case cleanup: Throwable => lifecycle.markFailed(cleanup)
      case None => ()
    frozen = if coordinator == null then None else coordinator.frozenFailure
    frozen.foreach(lifecycle.markFailed)
    if acquireAttempted && !releaseReturned && lifecycle.failure.isEmpty then
      lifecycle.markFailed(new IllegalStateException("owned writer responsibility retained without an admitted release"))
    val result = MySqlOwnedSessionCloseObservation(inputs.bootId, inputs.bootGeneration, connectionGeneration,
      verified != null || (openHolder != null && openHolder.hasRawHandle()), acquireAttempted, release, releaseAttempted, releaseReturned, jdbcCloseAttempted,
      jdbcClosed, frozen, lifecycle.failure, freshResponsibility, sessionBarrier, normalResponsibility,
      inspectionResult.orElse(recoveryActor.map(actor => MySqlCertifiedInspectionObservation("UNRESOLVED_INSPECTION_RESPONSIBILITY",
        None, None, None, actor.retainedNativeResponsibility, actor.rootRetained, actor.retainedFailure))))
    closeObserved = Some(result)
    lifecycle.finishClose()
    result

object MySqlOwnedDatabaseSession:
  /** 实际 owned IO 的独立 schema4 只读入口；成功仅消费完整 DATA，不创建旧3 session 或 Ready。 */
  private[mysql] def inspectScriptSchema(metadata: com.wi.floatingpod.config.MySqlRuntimeMetadata,
      inputs: MySqlScriptSchemaInputs, security: MySqlScriptSchemaAuditInputs,
      policy: MySqlRuntimeAuditPolicy, bootId: UUID, bootGeneration: Long,
      connectionGeneration: Long)(consume: MySqlScriptSchemaSnapshot => Unit): Unit =
    val owner = Thread.currentThread()
    require(metadata != null && inputs != null && security != null && policy != null && consume != null &&
      bootId != null && bootGeneration > 0L && connectionGeneration > 0L,
      "schema4 inspection requires complete independent configuration and actual owned generation")
    val pins = metadata.baseline()
    require(pins.schemaFormat() == MySqlScriptSchemaProfile.SchemaFormat && pins.coverage() == MySqlScriptSchemaProfile.Coverage &&
      metadata.schema() == inputs.schema && (security.limitProfile eq inputs.sourceLimitProfile) &&
      policy.expectedImport == MySqlExpectedNoImport && policy.expectedPhysicalFormat == MySqlExpectedPhysicalFormat.Format2,
      "schema4 inspection cannot borrow a format3 approval, source profile, import or physical target")
    MySqlScriptSchemaConfiguration.requireMatches(metadata, inputs, policy, security)
    val identity = MySqlApprovedBaselineIdentity(pins.format(), pins.schemaFormat(), pins.coverage(),
      metadata.expectedServerUuid(), pins.serverVersion(), metadata.schema(), pins.provisioningUuid(),
      metadata.expectedBindingUuid(), pins.ddlSha256(), pins.auditDefiner(), pins.sourceCaptureSha256())
    val baseline = MySqlApprovedBaselineLoader.load(pins.absolutePath(), pins.fileSha256(), identity)
    val expectation = new MySqlSchemaAuditExpectation(MySqlSchemaAuditProfile.ScriptPublication1,
      MySqlExpectedPhysicalFormat.Format2, identity.serverUuid, identity.serverVersion, identity.schema,
      metadata.expectedEffectiveUser(), policy.callerRoles, identity.provisioningUuid, identity.bindingUuid,
      identity.ddlSha256, identity.auditDefiner, policy.auditDefinerRoles, policy.expectedImport, baseline.objects,
      scriptSchemaInputs = Some(security))
    val settings = MySqlConnectionSettings(metadata.host(), metadata.port(), metadata.schema(), metadata.expectedServerUuid(),
      metadata.expectedEffectiveUser(), MySqlFileSources.credentials(owner, metadata.username(), metadata.credentialSourceRef()),
      metadata.pkcs12TrustStorePath(), MySqlFileSources.trustSecret(owner, metadata.trustSecretSourceRef()),
      metadata.connectTimeout(), metadata.socketTimeout(),
      MySqlDurabilityExpectation(metadata.innodbFlushLogAtTrxCommit(), metadata.syncBinlog()))
    val ledger = new MySqlFormat3AllocationLedger(inputs.sourceLimitProfile.maxRetainedBytes())
    val root = inputs.newAllocationRoot(inputs.budget.packetBytes, ledger)
    var verified: VerifiedMySqlConnection = null
    var primary: Throwable = null
    try
      verified = new MySqlConnectionSource(settings).open()
      val scope = MySqlAuditBootScope.forOwnedIo(verified, owner, bootId, bootGeneration, connectionGeneration)
      verified.acquireSessionBarrier(expectation, scope, Duration.ZERO) match
        case MySqlSessionBarrierAcquisition.Held(_) => ()
        case MySqlSessionBarrierAcquisition.Deferred =>
          throw new java.sql.SQLTransientConnectionException("schema4 inspection session barrier busy; no admission")
      val auditor = new MySqlSchemaAuditor(expectation, policy.maxReceiptAge)
      MySqlFreshWorldBootstrap.inspectScriptSchema(verified, scope, expectation, inputs, auditor, root)(consume)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      val cleanup = Vector.newBuilder[() => Unit]
      cleanup += (() => root.closeRootReadBudget())
      if verified != null then cleanup += (() => verified.close())
      MySqlFormat3BudgetCleanup.run(primary, cleanup.result())

  def openCertified(
      inputs: MySqlRuntimeInputs, connectionGeneration: Long, leaseDuration: Duration
  ): MySqlOwnedDatabaseSession =
    openCertifiedWithConnectionSourceForAssembly(inputs, connectionGeneration, leaseDuration,
      new MySqlConnectionSource(inputs.settings))

  /** 原 source 的 driver seam；仍执行真实 Session 初始化与失败清理。 */
  private[mysql] def openCertifiedWithConnectionSourceForAssembly(
      inputs: MySqlRuntimeInputs, connectionGeneration: Long, leaseDuration: Duration,
      source: MySqlConnectionSource
  ): MySqlOwnedDatabaseSession =
    require(source != null, "original connection source required")
    openCertifiedWithVerifiedConnectionSourceForAssembly(inputs, connectionGeneration, leaseDuration, () => source.open(), source)

  /** Package assembly retains the complete scope/barrier/audit/acquire/cleanup path. */
  private[mysql] def openCertifiedWithVerifiedConnectionSourceForAssembly(
      inputs: MySqlRuntimeInputs, connectionGeneration: Long, leaseDuration: Duration,
      openVerifiedConnection: () => VerifiedMySqlConnection,
      registeredSource: MySqlConnectionSource = null
  ): MySqlOwnedDatabaseSession =
    require(inputs != null && (Thread.currentThread() eq inputs.ownedIoThread), "session open requires exact actual IO owner")
    require(connectionGeneration > 0L, "connection generation must be positive")
    require(leaseDuration != null && !leaseDuration.isNegative && !leaseDuration.isZero, "explicit lease must be positive")
    val nanos = leaseDuration.toNanos
    require(nanos > 0L && nanos <= MySqlSchemaAuditor.MaxReceiptAgeNanos && nanos % 1000L == 0L,
      "explicit lease must be bounded and DATETIME(6) precise")
    val session = new MySqlOwnedDatabaseSession(inputs, connectionGeneration, leaseDuration)
    session.initializeWithVerifiedConnectionSource(
      if registeredSource == null then openVerifiedConnection else () => session.openRegisteredConnection(registeredSource))
    session

  /** 不打开资源；只有actual worker可分配，adapter必须先登记再initializeFresh。 */
  private[mysql] def allocateFresh(inputs: MySqlRuntimeInputs, connectionGeneration: Long,
                                  leaseDuration: Duration, request: MySqlFreshBootRequest): MySqlOwnedDatabaseSession =
    require(inputs != null && (Thread.currentThread() eq inputs.ownedIoThread), "Fresh allocation requires exact actual IO owner")
    require(connectionGeneration > 0L, "connection generation must be positive")
    require(leaseDuration != null && !leaseDuration.isNegative && !leaseDuration.isZero, "explicit lease must be positive")
    val nanos = leaseDuration.toNanos
    require(nanos > 0L && nanos <= MySqlSchemaAuditor.MaxReceiptAgeNanos && nanos % 1000L == 0L,
      "explicit lease must be bounded and DATETIME(6) precise")
    inputs.format3Contract.requireFreshPlan(request.plan)
    MySqlFreshBootRules.validate(request, inputs.approvedBaseline.identity, inputs.auditPolicy.expectedImport,
      inputs.auditPolicy.expectedPhysicalFormat, inputs.bootId, inputs.bootGeneration, inputs.originalMetadata)
    new MySqlOwnedDatabaseSession(inputs, connectionGeneration, leaseDuration, Some(request))

  /** 调用方已保留同lease；本allocator不构造JDBC/native库。 */
  private[mysql] def allocateFreshV2(inputs: MySqlRuntimeInputs, connectionGeneration: Long,
      leaseDuration: Duration, request: MySqlFreshBootRequest, lease: MySqlRuntimeInputs.ConsumerLease,
      ledger: MySqlFormat3AllocationLedger): MySqlOwnedDatabaseSession =
    require(inputs != null && inputs.hasV2Origin && request != null && connectionGeneration > 0L,
      "explicit V2 Fresh allocation inputs required")
    lease.requireFor(inputs,ledger)
    require(leaseDuration != null && !leaseDuration.isNegative && !leaseDuration.isZero,
      "explicit Fresh lease required")
    val nanos=leaseDuration.toNanos
    require(nanos>0L && nanos<=MySqlSchemaAuditor.MaxReceiptAgeNanos && nanos%1000L==0L,"bounded precise Fresh lease required")
    inputs.format3Contract.requireFreshPlan(request.plan)
    inputs.assemblyOwnerV2.withApprovedConfigurationWorkspace {
      MySqlFreshBootRules.validateV2(request,inputs.approvedBaseline.identity,inputs.auditPolicy.expectedImport,
        inputs.auditPolicy.expectedPhysicalFormat,inputs.bootId,inputs.bootGeneration,inputs.originalMetadataV2)
    }
    new MySqlOwnedDatabaseSession(inputs,connectionGeneration,leaseDuration,Some(request),None,None,lease,ledger)

  private[mysql] def allocateCertifiedOrdinaryV2(inputs: MySqlRuntimeInputs, connectionGeneration: Long,
      leaseDuration: Duration, request: MySqlOrdinaryCertifiedBootRequest, lease: MySqlRuntimeInputs.ConsumerLease,
      ledger: MySqlFormat3AllocationLedger): MySqlOwnedDatabaseSession =
    require(inputs != null && inputs.hasV2Origin && request != null && connectionGeneration>0L,
      "explicit V2 Ordinary allocation inputs required")
    lease.requireFor(inputs,ledger)
    require(leaseDuration != null && !leaseDuration.isNegative && !leaseDuration.isZero,"explicit Ordinary lease required")
    val nanos=leaseDuration.toNanos
    require(nanos>0L && nanos<=MySqlSchemaAuditor.MaxReceiptAgeNanos && nanos%1000L==0L,"bounded precise Ordinary lease required")
    require(request.boot.bootId()==inputs.bootId && request.boot.generation()==inputs.bootGeneration,"same assembled V2 boot required")
    inputs.format3Contract.requireOriginalCreation(request.approvedOriginalCreation)
    require(request.controlledMetadata.approvedConfigurationSha256.equalsIgnoreCase(request.approvedOriginalCreation.configurationSha256()),
      "Ordinary request must preserve full approved A")
    new MySqlOwnedDatabaseSession(inputs,connectionGeneration,leaseDuration,None,None,Some(CertifiedSourceRequest.Ordinary(request)),lease,ledger)

  /** 无资源分配；原creation身份不从observed DB生成。 */
  private[mysql] def allocateNormalPreGenesis(inputs: MySqlRuntimeInputs, connectionGeneration: Long,
                                  leaseDuration: Duration, request: MySqlNormalBootRequest): MySqlOwnedDatabaseSession =
    require(inputs != null && (Thread.currentThread() eq inputs.ownedIoThread), "Normal allocation requires exact actual IO owner")
    require(connectionGeneration > 0L, "connection generation must be positive")
    require(leaseDuration != null && !leaseDuration.isNegative && !leaseDuration.isZero, "explicit lease must be positive")
    val nanos = leaseDuration.toNanos
    require(nanos > 0L && nanos <= MySqlSchemaAuditor.MaxReceiptAgeNanos && nanos % 1000L == 0L,
      "explicit lease must be bounded and DATETIME(6) precise")
    inputs.format3Contract.requireFreshPlan(request.plan)
    MySqlNormalBootRules.validate(request, inputs.approvedBaseline.identity, inputs.auditPolicy.expectedImport,
      inputs.auditPolicy.expectedPhysicalFormat, inputs.bootId, inputs.bootGeneration, inputs.originalMetadata)
    new MySqlOwnedDatabaseSession(inputs, connectionGeneration, leaseDuration, None, Some(request))

  /** Allocator has no JDBC/native action; caller must publish this holder before initialize. */
  private[mysql] def allocateCertifiedInspection(inputs: MySqlRuntimeInputs, connectionGeneration: Long,
      request: MySqlCertifiedRestoreRequest): MySqlOwnedDatabaseSession =
    require(inputs != null && (Thread.currentThread() eq inputs.ownedIoThread) && connectionGeneration > 0L && request != null,
      "actual owned inspection allocation identity required")
    inputs.format3Contract.requireFreshPlan(request.plan) // original source identity DATA, not Fresh authority
    new MySqlOwnedDatabaseSession(inputs, connectionGeneration, Duration.ZERO, None, None,
      Some(CertifiedSourceRequest.Inspection(request)))

  /** No JDBC/native allocation; adapter retains this same registered actor before initialization. */
  private[mysql] def allocateCertifiedOrdinary(inputs: MySqlRuntimeInputs, connectionGeneration: Long,
      leaseDuration: Duration, request: MySqlOrdinaryCertifiedBootRequest): MySqlOwnedDatabaseSession =
    require(inputs != null && (Thread.currentThread() eq inputs.ownedIoThread) && connectionGeneration > 0L && request != null,
      "actual owned ordinary allocation identity required")
    require(leaseDuration != null && !leaseDuration.isNegative && !leaseDuration.isZero,
      "explicit ordinary writer lease must be positive")
    val nanos = leaseDuration.toNanos
    require(nanos > 0L && nanos <= MySqlSchemaAuditor.MaxReceiptAgeNanos && nanos % 1000L == 0L,
      "ordinary writer lease must be bounded and DATETIME(6) precise")
    require(request.boot.bootId() == inputs.bootId && request.boot.generation() == inputs.bootGeneration,
      "ordinary request must retain actual assembled boot generation")
    require(request.approvedOriginalCreation.plan().schemaFormat() == inputs.approvedBaseline.identity.schemaFormat &&
      request.approvedOriginalCreation.plan().physicalFormat() == inputs.auditExpectation.expectedPhysicalFormat.version &&
      request.approvedOriginalCreation.plan().schema().equals(inputs.approvedBaseline.identity.schema),
      "ordinary original birth plan must match the actual approved format3/physical2 baseline identity")
    require(request.controlledMetadata.approvedConfigurationSha256.equalsIgnoreCase(
      request.approvedOriginalCreation.configurationSha256()),
      "ordinary request configuration must retain the approved original source pin")
    new MySqlOwnedDatabaseSession(inputs, connectionGeneration, leaseDuration, None, None,
      Some(CertifiedSourceRequest.Ordinary(request)))
