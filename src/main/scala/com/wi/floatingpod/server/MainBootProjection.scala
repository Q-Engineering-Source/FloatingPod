package com.wi.floatingpod.server

import com.wi.floatingpod.bootstrap.{ProductionBootstrapInstallation, ProductionDedicatedBootstrapCallbacks}
import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.domain.{FloatingPodState, PlayerEntitlement}
import com.wi.floatingpod.storage.*
import com.wi.floatingpod.storage.mysql.*
import net.minecraft.server.MinecraftServer
import net.minecraft.world.WorldServer
import net.minecraft.world.chunk.Chunk
import net.minecraft.world.gen.ChunkProviderServer
import com.wi.floatingpod.server.io.{IoOperationTag, IoSubmission}
import java.util.UUID

/** 原 Boot 输入的生命周期，不替代后续 critical ACK/current runtime projection。 */
private[server] final class MainBootReadLifetime(owner: Thread, initialEpoch: Long):
  require(owner != null && initialEpoch >= 0L, "actual Main and accepted epoch required")
  private var failure = Option.empty[Throwable]
  private var completedDrop = Option.empty[String]
  def retainedFailure: Option[Throwable] =
    require(Thread.currentThread() eq owner, "Boot read observation requires original Main")
    failure
  def fail(cause: Throwable): Nothing =
    require(Thread.currentThread() eq owner, "Boot read failure requires original Main")
    require(cause != null, "original Boot read failure required")
    if failure.isEmpty then failure = Some(cause)
    throw failure.get
  def requireCurrent(epoch: Long): Unit =
    require(Thread.currentThread() eq owner, "Boot reads require exact claimed Main")
    failure.foreach(cause => throw cause)
    require(completedDrop.isEmpty, "Boot read input was normally retired: " + completedDrop.getOrElse("unknown"))
    if epoch != initialEpoch then fail(new IllegalStateException("Boot read input retired by an accepted repository mutation"))
  /** Normal continuation retirement revokes only this DATA read qualification; it is not a failure or epoch refresh. */
  private[server] def dropForSourceContinuation(epoch: Long): Unit =
    requireCurrent(epoch)
    completedDrop = Some("source continuation")
  private[server] def requireResponsibilityOwner(): Unit =
    require(Thread.currentThread() eq owner, "original Main source responsibility required")
    failure.foreach(cause => throw cause)
  /** Ordinary 的原读者已由原 Main 结束；合法 epoch 退休不重新取得旧读资格。 */
  private[server] def dropOrdinaryOwnedInput(): Unit =
    require(Thread.currentThread() eq owner, "Boot input drop requires original Main")
    failure.foreach(throw _)
    require(completedDrop.isEmpty, "Original Boot input drop is one-shot")
    completedDrop = Some("ordinary application retirement")
  /** 正常 drop 不是失败；旧读者仍须拒绝，不清除既有 failure 或重签 epoch。 */
  private[server] def dropForCompletion(epoch: Long): Unit =
    requireCurrent(epoch)
    completedDrop = Some("completion")

/** 真 consumer 使用的 DATA 生命周期校验；本 core 不发行 callbacks/SQL/Ready 资格。 */
private[server] object MainBootReadLifetime:
  def fromOriginalSource(owner: Thread, source: MySqlOwnedFormat3Snapshot,
      actualBootTag: IoOperationTag, observedCurrentEpoch: Long): MainBootReadLifetime =
    require(source != null, "original Boot source required")
    val original = new MainBootReadLifetime(owner, source.requireOriginalBootEpoch(actualBootTag))
    original.requireCurrent(observedCurrentEpoch) // 首次消费前已变化同样拒绝，不重 mint。
    original

/** 构造只有 actual retained callbacks 的 one-shot consumer；没有 public DTO/boolean issuer。 */
final class MainBootProjection private (
    private val callbacks: ProductionDedicatedBootstrapCallbacks,
    private var reply: MySqlProductionBootReply,
    private var data: MySqlBootMainReadData,
    private val lifetime: MainBootReadLifetime
) extends MainPodReadAccess:
  val identity: MySqlOwnedIoIdentity = callbacks.adapter.identity
  private var profileInput: SourceLimitProfile = MySqlProductionBootReply.actualSourceLimitProfile(reply)
  private var snapshotInput: MySqlOwnedFormat3Snapshot = MySqlProductionBootReply.fullTwentyRelationSnapshot(reply)
  private var recallSourceCustodyAttempted = false
  private var recallSourceCustody: Option[NormalRecallRecoverySourceCustody] = None
  private var recallSourceCustodyFailure: Throwable = null
  private var recallSourceCustodyLink: MySqlOwnedIoAdapter.RecallRecoveryLink = null
  private var recallSourceCustodyRoot: com.wi.floatingpod.server.source.SourceForestAllocationBudget.Root = null
  private var sourceContinuationAttempted = false
  private var sourceContinuationLease: MainBootProjection.SourceContinuationLease = null
  private var sourceContinuationFailure: Throwable = null
  private var sourceContinuationLink: MySqlOwnedIoAdapter.RecallRecoveryLink = null
  def limitProfile: SourceLimitProfile =
    require(profileInput != null, "Original Main profile input was dropped")
    profileInput
  /** 只核 Main 装配的原 adapter 引用，不发行新 Boot/read/SQL 资格。 */
  private[server] def requireOriginalOwnedAdapter(actual: MySqlOwnedIoAdapter): Unit =
    require((Thread.currentThread() eq callbacks.actualServer.getServerThread) &&
      (actual eq callbacks.adapter), "original Main Boot adapter reference required")
  def bootSnapshot: MySqlOwnedFormat3Snapshot =
    require(snapshotInput != null, "Original Main snapshot input was dropped")
    snapshotInput
  /** 只撤本 Main consumer；worker snapshot/root 仍由原 worker owner 结算。 */
  private[floatingpod] def dropOrdinaryOwnedInput(original: ProductionDedicatedBootstrapCallbacks): Unit =
    require((original eq callbacks) && (Thread.currentThread() eq callbacks.actualServer.getServerThread),
      "Original Ordinary Main input owner required")
    reply match
      case _: MySqlOwnedIoReply.OrdinaryCertifiedBooted => ()
      case deployment: MySqlOwnedIoReply.DeploymentReceiptBooted
          if deployment.owned.phase==MySqlProductionBootReply.DeploymentReceiptPhase.OrdinaryAdmitted => ()
      case _ => throw new IllegalStateException("Original OrdinaryCertified Main input required")
    lifetime.dropOrdinaryOwnedInput()
    data = null
    reply = null
    profileInput = null
    snapshotInput = null
  private[floatingpod] def requireOrdinaryOwnedInputDropped(original: ProductionDedicatedBootstrapCallbacks): Unit =
    require((original eq callbacks) && (Thread.currentThread() eq callbacks.actualServer.getServerThread),
      "Original Ordinary Main input owner required")
    lifetime.retainedFailure.foreach(throw _)
    require(data == null && reply == null && profileInput == null && snapshotInput == null,
      "Original Main input fields remain retained")
  val activeLimit: Int = callbacks.approvedActiveLimit
  private def current(): Unit =
    require(Thread.currentThread() eq callbacks.actualServer.getServerThread, "original dedicated Main required")
    lifetime.requireCurrent(callbacks.adapter.repositorySubmissionEpoch)
    val observed = reply match
      case recovering: MySqlOwnedIoReply.RecallRecoveringBooted => callbacks.adapter.pollRecallMaintenance(recovering.recovery)
      case recovering:MySqlOwnedIoReply.DeploymentReceiptRecallRecoveringBooted => callbacks.adapter.pollRecallMaintenance(recovering.owned.recovery)
      case _ => callbacks.adapter.pollAdmissionMaintenance
    observed.failure.foreach(lifetime.fail)
    val checkpoint = callbacks.adapter.checkpoint.io
    checkpoint.fatalCause.foreach(lifetime.fail)
    if !checkpoint.accepting || !observed.isCurrent then
      lifetime.fail(new IllegalStateException("Boot read admission freshness is unavailable"))
    val latest = observed.latest.get
    val (_, _, _, bindingId, writerEpoch) = MySqlProductionBootReply.sessionIdentity(reply)
    if !data.binding.exists(_.bindingUuid == bindingId) || latest.bootId != identity.bootId || latest.bootGeneration != identity.bootGeneration ||
      latest.connectionGeneration != identity.connectionGeneration || latest.bindingId != bindingId ||
      latest.writerEpoch != writerEpoch || latest.schema != bootSnapshot.schema ||
      latest.serverId != bootSnapshot.serverUuid then
      lifetime.fail(new IllegalStateException("Boot read admission belongs to another exact session"))
  /** 仅检查读租约，不发行 Ready、absence、ticket 或 item mutation 资格。 */
  private[floatingpod] def requireCurrentReads(): Unit = current()
  private[floatingpod] def retainConsumerFailure(cause: Throwable): Nothing = lifetime.fail(cause)
  /** 仅选择已认证 Fresh 准备阶段；不发行 completion、Ready 或退出许可。 */
  private[floatingpod] def isCertifiedFreshPreparation: Boolean =
    current()
    reply match
      case deployment: MySqlOwnedIoReply.DeploymentReceiptBooted =>
        require(data.binding.exists(_.cutoverState == PhysicalRecoveryCutoverState.Certified),
          "actual schema5 completion requires its original Certified binding")
        deployment.owned.phase == MySqlProductionBootReply.DeploymentReceiptPhase.FreshCertifiedPreparation
      case _: MySqlOwnedIoReply.FreshBooted =>
        require(data.binding.exists(_.cutoverState == PhysicalRecoveryCutoverState.Certified),
          "actual Fresh preparation requires its original Certified binding")
        true
      case _: MySqlOwnedIoReply.OrdinaryCertifiedBooted => false
      case _: MySqlOwnedIoReply.RecallRecoveringBooted => false
      case _:MySqlOwnedIoReply.DeploymentReceiptRecallRecoveringBooted => false
  private[floatingpod] def recallRecoveryLink: Option[MySqlOwnedIoAdapter.RecallRecoveryLink] =
    current()
    reply match
      case recovering: MySqlOwnedIoReply.RecallRecoveringBooted => Some(recovering.recovery)
      case recovering:MySqlOwnedIoReply.DeploymentReceiptRecallRecoveringBooted => Some(recovering.owned.recovery)
      case _ => None
  /** 只撤销此原 Main projection；不是 session/native 关闭证明，也不签任何停止许可。 */
  private[floatingpod] def dropFreshReadInputForCompletion(): MainBootProjection.FreshReadDrop =
    require(isCertifiedFreshPreparation, "original Fresh reply required for preparation completion")
    val epoch = callbacks.adapter.repositorySubmissionEpoch
    val receipt = new MainBootProjection.FreshReadDrop(this, callbacks, bootSnapshot, callbacks.bootTag, epoch)
    lifetime.dropForCompletion(epoch)
    data = null
    reply = null
    receipt
  private[server] def claimedActiveRecoveryBlock: Option[String] =
    current()
    reply match
      case deployment: MySqlOwnedIoReply.DeploymentReceiptBooted => deployment.owned.phase match
        case MySqlProductionBootReply.DeploymentReceiptPhase.FreshCertifiedPreparation =>
          Some("Fresh schema5 preparation requires its original dedicated completion exit")
        case MySqlProductionBootReply.DeploymentReceiptPhase.OrdinaryAdmitted => None
      case _: MySqlOwnedIoReply.FreshBooted => Some("Fresh Boot recovery obligations require their original owner; claimed ROW_ACTIVE recovery is incomplete")
      case _: MySqlOwnedIoReply.OrdinaryCertifiedBooted => None
      case _: MySqlOwnedIoReply.RecallRecoveringBooted => Some("original Recall recovery must finish before ordinary active recovery")
      case _:MySqlOwnedIoReply.DeploymentReceiptRecallRecoveringBooted => Some("original schema5 Recall recovery must finish before ordinary active recovery")
  /** 原 Ordinary reply 的只读发布资格；不消耗新 reply、不把 inventory Permit 当 Ready。 */
  private[floatingpod] def ordinaryPublicationBinding: PhysicalRecoveryBinding =
    current()
    reply match
      case _: MySqlOwnedIoReply.OrdinaryCertifiedBooted => ()
      case deployment: MySqlOwnedIoReply.DeploymentReceiptBooted
          if deployment.owned.phase==MySqlProductionBootReply.DeploymentReceiptPhase.OrdinaryAdmitted => ()
      case _ => throw new IllegalStateException("OrdinaryCertified Boot required for debug publication")
    data.binding.filter(_.cutoverState == PhysicalRecoveryCutoverState.Certified).getOrElse(
      throw new IllegalStateException("Original Certified binding required for debug publication"))
  private[floatingpod] def retainRecallSourceCustody(server:MinecraftServer):Option[NormalRecallRecoverySourceCustody] =
    require(server eq callbacks.actualServer,"original Recall source server required")
    require(Thread.currentThread() eq server.getServerThread,"original Recall source Main required")
    if recallSourceCustodyAttempted then
      if recallSourceCustodyFailure != null then throw recallSourceCustodyFailure
      require(recallSourceCustodyLink != null || recallSourceCustody.isEmpty,
        "retained Recall source custody lost its original link")
      recallSourceCustody.foreach(_.requireAllocationResponsibility())
      return recallSourceCustody
    current()
    val link=recallRecoveryLink.getOrElse(return None)
    recallSourceCustodyAttempted = true
    recallSourceCustodyLink = link
    val allocation = link.sourceAllocation
    recallSourceCustodyRoot = allocation.orNull
    try
      recallSourceCustody = allocation.map { rootAllocation =>
        NormalRecallRecoverySourceCustody.retain(this,server,callbacks.worldContext,link,limitProfile,rootAllocation)
      }
      recallSourceCustody
    catch
      case cause: Throwable =>
        recallSourceCustodyFailure = cause
        throw cause

  /** Retain one exact continuation keeper while the original Boot DATA read is still current. */
  private[floatingpod] def retainRecallSourceContinuation(server: MinecraftServer,
      exactLink: MySqlOwnedIoAdapter.RecallRecoveryLink): MainBootProjection.SourceContinuationLease =
    require(server eq callbacks.actualServer, "original Recall continuation server required")
    require(Thread.currentThread() eq server.getServerThread, "original Recall continuation Main required")
    if sourceContinuationAttempted then
      if sourceContinuationFailure != null then throw sourceContinuationFailure
      require(sourceContinuationLease != null && (sourceContinuationLink eq exactLink),
        "original restricted continuation lease was not retained for this exact link")
      sourceContinuationLease.requireResponsibilityOwner(server, exactLink)
      return sourceContinuationLease
    current()
    val actualLink = recallRecoveryLink.getOrElse(
      throw new IllegalStateException("original restricted Recall Boot link missing"))
    require((actualLink eq exactLink) && (exactLink.identity eq identity) &&
      exactLink.sourceAllocation.nonEmpty, "original source continuation link/account required")
    require(recallSourceCustodyAttempted && recallSourceCustodyFailure == null,
      "the original preinitialized source custody/account must exist before its continuation lease")
    val custody = recallSourceCustody.getOrElse(
      throw new IllegalStateException("original restricted source custody/account missing"))
    val root = exactLink.sourceAllocation.get
    require(recallSourceCustodyRoot eq root, "source continuation must retain the exact custody root")
    sourceContinuationAttempted = true
    sourceContinuationLink = exactLink
    try
      val result = MainBootProjection.SourceContinuationLease.retain(
        this, callbacks, server, reply, snapshotInput, exactLink, root, custody, limitProfile,
        callbacks.adapter.repositorySubmissionEpoch)
      sourceContinuationLease = result
      custody.bindSourceContinuationLease(result)
      callbacks.registerRecallSourceContinuation(this, result, server, reply, exactLink, root, custody, limitProfile)
      val exactProtectionIndex = data.sourceProtection
      val retainedProtection = custody.retainProtectionForContinuation(this, result, exactProtectionIndex)
      result.retainProtectionData(custody, exactProtectionIndex, retainedProtection, result.originalEpoch)
      result
    catch
      case cause: Throwable =>
        sourceContinuationFailure = cause
        if sourceContinuationLease != null then sourceContinuationLease.retainRegistrationFailure(cause)
        throw cause

  private[floatingpod] def retainAvailableRecallSourceContinuation(server: MinecraftServer):
      Option[MainBootProjection.SourceContinuationLease] =
    require(server eq callbacks.actualServer, "original Recall continuation server required")
    recallRecoveryLink match
      case Some(link) if link.sourceAllocation.nonEmpty => Some(retainRecallSourceContinuation(server, link))
      case _ => None

  private[floatingpod] def retireBootReadForSourceContinuation(
      actualLease: MainBootProjection.SourceContinuationLease, expectedEpoch: Long): Unit =
    require(actualLease != null && (sourceContinuationLease eq actualLease) &&
      actualLease.belongsToProjection(this,callbacks),
      "exact original source continuation lease required")
    actualLease.requireResponsibilityOwner(callbacks.actualServer, actualLease.link)
    lifetime.dropForSourceContinuation(expectedEpoch)
    data = null
    reply = null
    profileInput = null
    snapshotInput = null

  /** 仅原 publication 装配时保留已核事实；之后不能为 normal stop 重借旧 Boot epoch。 */
  private[server] def ordinaryRecallSourceFacts: MySqlNormalRecallSourceIdentity =
    current()
    reply match
      case ordinary: MySqlOwnedIoReply.OrdinaryCertifiedBooted =>
        val facts = ordinary.owned.ordinaryFacts
        require((facts.originalCreation eq callbacks.approvedOriginalCreation) &&
          facts.currentBootUuid == identity.bootId && facts.currentBootGeneration == identity.bootGeneration &&
          facts.connectionGeneration == identity.connectionGeneration &&
          facts.sourceWorldIdentity == facts.originalCreation.sourceWorldIdentity(),
          "original Ordinary source facts required before publication")
        MySqlNormalRecallSourceIdentity(
          facts.originalCreation.sourceWorldIdentity(),facts.sourceWorldIdentity)
      case deployment:MySqlOwnedIoReply.DeploymentReceiptBooted =>
        require(deployment.owned.phase==MySqlProductionBootReply.DeploymentReceiptPhase.OrdinaryAdmitted &&
          (deployment.owned.identity eq identity),"original Ordinary schema5 publication source required")
        deployment.owned.normalRecallSourceFacts.getOrElse(
          throw new IllegalStateException("original Ordinary schema5 source facts were not retained"))
      case _ => throw new IllegalStateException("Normal recall source requires original Ordinary Boot")
  def retainedFailure: Option[Throwable] = lifetime.retainedFailure
  def findStored(id: UUID): Option[StoredPodRow] = { current(); data.stored(id) }
  def findState(id: UUID): Option[FloatingPodState] = { current(); data.state(id) }
  def findEntitlement(id: UUID): Option[PlayerEntitlement] = { current(); data.entitlement(id) }
  def recoveryRows: Vector[StoredPodRow] = { current(); data.recoveryRows }
  def desiredActiveRows: Vector[StoredPodRow] = { current(); data.desiredActiveRows }
  def activeCount: Int = { current(); data.activeCount }
  def activeCountFor(id: UUID): Int = { current(); data.activeCountFor(id) }
  def binding: Option[PhysicalRecoveryBinding] = { current(); data.binding }
  def recoveryLocators: Vector[PhysicalRecoveryLocator] = { current(); data.locators }
  private[server] def originalActiveAttemptFootprints: Map[UUID, (UUID, UUID)] =
    current()
    data.activeAttemptFootprints
  def unboundReceipt(id: UUID): Option[UnboundPreparationReceipt] = { current(); data.receipt(id) }
  /** 原 Boot SQL DATA 分类；不证明实际 world/loader 或并发 queue 未变，更不发行保存许可。 */
  private[floatingpod] def sourceProtection(dimension: Int, chunkX: Int, chunkZ: Int): MySqlBootSourceProtectionIndex.Classification =
    current()
    data.sourceProtection.classify(dimension, chunkX, chunkZ)
  private[floatingpod] def requireOriginalSourceProtectionIndex(
      exactIndex: MySqlBootSourceProtectionIndex): Unit =
    require(exactIndex != null, "exact original Boot source protection index required")
    current()
    require(data.sourceProtection eq exactIndex,
      "source protection index differs from the exact original Boot DATA")

object MainBootProjection:
  /** 原 projection 已实际丢弃其 decoded/reply 引用；接收方仍须结算全部其他持有者。 */
  final class FreshReadDrop private[MainBootProjection] (
      private val projection: MainBootProjection,
      private val callbacks: ProductionDedicatedBootstrapCallbacks,
      private[floatingpod] val snapshot: MySqlOwnedFormat3Snapshot,
      private[floatingpod] val tag: IoOperationTag,
      private[floatingpod] val epoch: Long):
    private[floatingpod] def requireOriginal(originalProjection: MainBootProjection,
        originalCallbacks: ProductionDedicatedBootstrapCallbacks): Unit =
      require((projection eq originalProjection) && (callbacks eq originalCallbacks) &&
        (Thread.currentThread() eq callbacks.actualServer.getServerThread), "original Fresh Main read drop required")

  /** Source observation is a Main-only identity join, never an alias for the original Boot rows. */
  private[floatingpod] final class SourceContinuationObservation private[MainBootProjection] (
      private[MainBootProjection] val lease: SourceContinuationLease,
      private[MainBootProjection] val domain: com.wi.floatingpod.server.source.SourceDomainCapture.CatalogDomainObservation,
      private[MainBootProjection] val maintenance: MySqlOwnedAdmissionFreshness,
      val settledRepositoryEpoch: Long,
      private[MainBootProjection] val sourceCompletion: Option[SourceCompletionAck]
  ):
    private[floatingpod] def belongsToLease(exact: SourceContinuationLease): Boolean = lease eq exact

  /** Exact Main-ACK evidence slot; this slice has no issuer, so retired-Boot actions stay blocked. */
  private[floatingpod] final class SourceCompletionAck private[MainBootProjection] (
      private val pending: SourceSubmissionPending,
      private val settledRepositoryEpoch: Long
  ):
    private[MainBootProjection] def requireFor(lease: SourceContinuationLease, expectedEpoch: Long): Unit =
      require(lease != null && pending != null, "exact source completion ACK required")
      pending.requireLease(lease)
      require(pending.submission == SourceContinuationLease.SubmissionState.Accepted &&
        settledRepositoryEpoch == pending.nextRepositoryEpoch && expectedEpoch == settledRepositoryEpoch,
        "source completion ACK does not settle the exact accepted pending epoch")

  /** Main-issued locator tied to one exact copied carrier in the retained full protection view. */
  private[floatingpod] final class SourceCarrierLocator private[MainBootProjection] (
      private val lease: SourceContinuationLease,
      private val observation: SourceContinuationObservation,
      private val holder: MySqlBootSourceProtectionIndex.RetainedData,
      private val holderIndex: Int,
      private val selectedDimension: Int,
      private val selectedChunkX: Int,
      private val selectedChunkZ: Int,
      private[floatingpod] val carrier: MySqlBootSourceProtectionIndex.Carrier
  ):
    private[floatingpod] def requireFor(actualLease: SourceContinuationLease,
        expectedObservation: SourceContinuationObservation,
        actualDomain: com.wi.floatingpod.server.source.SourceDomainCapture.CatalogDomainObservation):
        MySqlBootSourceProtectionIndex.Carrier =
      requireRetainedFor(actualLease, expectedObservation, actualDomain)
      actualLease.requireExactDomainObservation(expectedObservation, actualDomain)
      carrier

    /** Rechecks only the immutable holder/domain identities; per-chunk decisions never refresh the action gate. */
    private[floatingpod] def requireRetainedFor(actualLease: SourceContinuationLease,
        expectedObservation: SourceContinuationObservation,
        actualDomain: com.wi.floatingpod.server.source.SourceDomainCapture.CatalogDomainObservation):
        MySqlBootSourceProtectionIndex.Carrier =
      require((lease eq actualLease) && (observation eq expectedObservation) && holder != null && carrier != null,
        "exact original Main carrier locator/observation required")
      require(actualDomain != null && (observation.domain eq actualDomain),
        "exact captured Main domain observation required for carrier locator")
      require((holder eq actualLease.sourceProtectionData()) && holder.containsExactCarrier(carrier) &&
        (holder.carrierAt(holderIndex) eq carrier) && carrier.chunk.dimension == selectedDimension &&
        carrier.chunk.x == selectedChunkX && carrier.chunk.z == selectedChunkZ,
        "carrier locator differs from the exact retained protection holder and selected Main chunk")
      carrier

  /** Exact Fenced readonly-plan coverage, privately tied to the copied Boot view and its observation. */
  private[floatingpod] final class SourceChunkEffectClosure private[MainBootProjection] (
      lease: SourceContinuationLease,
      observation: SourceContinuationObservation,
      domain: com.wi.floatingpod.server.source.SourceDomainCapture.CatalogDomainObservation,
      carrierLocator: SourceCarrierLocator,
      planAck: MySqlOwnedIoAdapter.RestrictedSourcePlanAck,
      plan: MySqlFencedTransaction.SourceReservePlan5,
      barrier: com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission#SourceScopedBarrier,
      closure: MySqlFencedTransaction.SourceReserveEffectClosure5
  ):
    private val mainThread = Thread.currentThread()
    private var currentLease = lease
    private var currentObservation = observation
    private var currentDomain = domain
    private var currentCarrierLocator = carrierLocator
    private var currentPlanAck = planAck
    private var currentPlan = plan
    private var currentBarrier = barrier
    private var currentClosure = closure
    private var openPlanViews = 0
    private var revokedForDrop = false
    private var failedPlanViewCharge: AutoCloseable = null
    private var failedPlanViewCause: Throwable = null

    private[floatingpod] def requireFor(actualLease: SourceContinuationLease,
        expectedObservation: SourceContinuationObservation): Unit =
      require((currentLease eq actualLease) && (currentObservation eq expectedObservation) && currentDomain != null &&
        !revokedForDrop && currentCarrierLocator != null && currentPlanAck != null && currentPlan != null &&
        currentBarrier != null && currentClosure != null,
        "exact registered source continuation plan closure required")
      require(currentObservation.domain eq currentDomain,
        "source plan effect closure belongs to another captured domain observation")
      require((currentPlan.effectClosure() eq currentClosure) &&
        currentClosure.containsExactCarrier(currentCarrierLocator.carrier),
        "Fenced source plan closure lost its exact retained plan/carrier binding")

    /** 一个真实、短期的 Main 只读视图；全部视图关闭前不得 drop。 */
    private[MainBootProjection] def borrowPlanView(actualLease: SourceContinuationLease,
        expectedObservation: SourceContinuationObservation): BorrowedSourcePlanView = {
      require((Thread.currentThread() eq mainThread) && (currentLease eq actualLease) &&
        (currentObservation eq expectedObservation) &&
        !revokedForDrop && currentClosure != null,
        "exact live Main plan view is required")
      if failedPlanViewCause != null then throw failedPlanViewCause
      val nextViewCount = Math.addExact(openPlanViews, 1)
      val exactClosure = currentClosure
      val charge = actualLease.retainAdmissionCharge(expectedObservation, 256L,
        "Main borrowed source plan view+charge holder BEFORE allocation")
      var view: BorrowedSourcePlanView = null
      try
        view = new BorrowedSourcePlanView(this, exactClosure, charge)
      catch
        case cause: Throwable =>
          try charge.close()
          catch
            case cleanup: Throwable =>
              if cleanup ne cause then cause.addSuppressed(cleanup)
              failedPlanViewCharge = charge
              failedPlanViewCause = cause
          throw cause
      openPlanViews = nextViewCount
      view
    }

    private[MainBootProjection] def readPlanView(actualView: BorrowedSourcePlanView,
        exactClosure: MySqlFencedTransaction.SourceReserveEffectClosure5): MySqlFencedTransaction.SourceReserveEffectClosure5 = {
      require((Thread.currentThread() eq mainThread) && actualView != null &&
        actualView.isOpenFor(this) && !revokedForDrop && openPlanViews > 0 &&
        (currentClosure eq exactClosure),
        "actual borrowed Main plan view is closed, revoked, or foreign")
      exactClosure
    }

    private[MainBootProjection] def releasePlanView(actualView: BorrowedSourcePlanView,
        exactClosure: MySqlFencedTransaction.SourceReserveEffectClosure5): Unit = {
      require((Thread.currentThread() eq mainThread) && actualView != null &&
        actualView.isOpenFor(this) && !revokedForDrop && openPlanViews > 0 &&
        (currentClosure eq exactClosure),
        "actual borrowed Main plan view cannot be released from a foreign or revoked wrapper")
      openPlanViews = Math.subtractExact(openPlanViews, 1)
    }

    /** 借用 DATA 已清引用后，若退费失败则由原 wrapper 保留该费用责任并阻止 drop。 */
    private[MainBootProjection] def retainFailedPlanViewCharge(
        exactCharge: AutoCloseable, cause: Throwable): Unit = {
      require((Thread.currentThread() eq mainThread) && exactCharge != null && cause != null,
        "original Main and failed borrowed-view charge required")
      if failedPlanViewCharge == null then failedPlanViewCharge = exactCharge
      else if failedPlanViewCharge ne exactCharge then
        cause.addSuppressed(new IllegalStateException("another borrowed source plan view charge is already retained"))
      if failedPlanViewCause == null then failedPlanViewCause = cause
      else if failedPlanViewCause ne cause then failedPlanViewCause.addSuppressed(cause)
    }

    private[MainBootProjection] def isRevokedForDrop: Boolean = {
      (Thread.currentThread() eq mainThread) && (currentLease == null) &&
        (currentObservation == null) && currentDomain == null &&
        revokedForDrop && openPlanViews == 0 &&
        currentCarrierLocator == null && currentPlanAck == null && currentPlan == null &&
        currentBarrier == null && currentClosure == null && failedPlanViewCharge == null
    }

    private[MainBootProjection] def requireNoActivePlanViewsForDrop(actualLease: SourceContinuationLease,
        expectedObservation: SourceContinuationObservation): Unit = {
      require((Thread.currentThread() eq mainThread) && (currentLease eq actualLease) &&
        (currentObservation eq expectedObservation) &&
        !revokedForDrop && currentClosure != null,
        "exact live Main source plan wrapper required before drop")
      if failedPlanViewCause != null then throw failedPlanViewCause
      require(failedPlanViewCharge == null && openPlanViews == 0,
        "active or incompletely released Main plan view prevents source plan drop")
    }

    /** Main 在 Adapter 请求 worker drop receipt 前撤销所有可达的只读视图。 */
    private[MainBootProjection] def revokeViewsForDrop(actualLease: SourceContinuationLease,
        expectedObservation: SourceContinuationObservation,
        metadataCharge: AutoCloseable): SourcePlanViewsRevoked = {
      require((Thread.currentThread() eq mainThread) && (currentLease eq actualLease) &&
        (currentObservation eq expectedObservation) && !revokedForDrop && openPlanViews == 0 &&
        currentCarrierLocator != null && currentPlanAck != null && currentPlan != null &&
        currentBarrier != null && currentClosure != null && metadataCharge != null,
        "exact unborrowed Main source plan view is required before drop")
      if failedPlanViewCause != null then throw failedPlanViewCause
      require(failedPlanViewCharge == null,
        "failed Main source plan view charge cleanup retains the original drop responsibility")
      val exactPlanAck = currentPlanAck
      val exactPlan = currentPlan
      val exactLease = currentLease
      val exactObservation = currentObservation
      val dropTag = actualLease.newSourcePlanDropTag()
      val proof = new SourcePlanViewsRevoked(exactLease, exactObservation, this, exactPlanAck, exactPlan,
        dropTag, metadataCharge)
      revokedForDrop = true
      currentLease = null
      currentObservation = null
      currentDomain = null
      currentCarrierLocator = null
      currentPlanAck = null
      currentPlan = null
      currentBarrier = null
      currentClosure = null
      proof
    }

  /** 可关闭的借用只读视图，只暴露 closure DATA，不是 SQL owner 句柄。 */
  private[floatingpod] final class BorrowedSourcePlanView private[MainBootProjection] (
      private var wrapper: SourceChunkEffectClosure,
      private var closure: MySqlFencedTransaction.SourceReserveEffectClosure5,
      private var charge: AutoCloseable
  ) extends AutoCloseable:
    private var open = true
    private var chargeReleased = false
    private var closeFailure: Throwable = null
    private[MainBootProjection] def isOpenFor(actualWrapper: SourceChunkEffectClosure): Boolean =
      open && (wrapper eq actualWrapper)
    private def currentClosure(): MySqlFencedTransaction.SourceReserveEffectClosure5 =
      require(open && closeFailure == null, "closed or failed Main source plan view cannot be read")
      val exactWrapper = Option(wrapper).getOrElse(
        throw new IllegalStateException("open Main source plan view lost its wrapper"))
      val exactClosure = Option(closure).getOrElse(
        throw new IllegalStateException("open Main source plan view lost its exact DATA"))
      exactWrapper.readPlanView(this, exactClosure)
    private[floatingpod] def globalUnresolved: Boolean =
      currentClosure().globalUnresolved
    private[floatingpod] def containsChunk(dimension: Int, chunkX: Int, chunkZ: Int): Boolean =
      currentClosure().containsChunk(dimension, chunkX, chunkZ)
    override def close(): Unit = {
      if closeFailure != null then throw closeFailure
      if open then
        val exactClosure = Option(closure).getOrElse(
          throw new IllegalStateException("open Main source plan view lost its exact DATA"))
        val exactWrapper = Option(wrapper).getOrElse(
          throw new IllegalStateException("open Main source plan view lost its wrapper"))
        try exactWrapper.releasePlanView(this, exactClosure)
        catch
          case cause: Throwable =>
            closeFailure = cause
            throw cause
        closure = null
        wrapper = null
        open = false
        if !chargeReleased then
          val exactCharge = Option(charge).getOrElse(
            throw new IllegalStateException("closed Main source plan view lost its admission charge"))
          try exactCharge.close()
          catch
            case cause: Throwable =>
              closeFailure = cause
              try exactWrapper.retainFailedPlanViewCharge(exactCharge, cause)
              catch
                case retentionFailure: Throwable =>
                  if retentionFailure ne cause then cause.addSuppressed(retentionFailure)
                  throw cause
              charge = null
              throw cause
          charge = null
          chargeReleased = true
    }

  /** Main 签发的证明，绑定已撤销的精确 plan wrapper 与其借用 DATA 视图。 */
  private[floatingpod] final class SourcePlanViewsRevoked private[MainBootProjection] (
      private var lease: SourceContinuationLease,
      private var observation: SourceContinuationObservation,
      private var overlay: SourceChunkEffectClosure,
      private var planAck: MySqlOwnedIoAdapter.RestrictedSourcePlanAck,
      private var plan: MySqlFencedTransaction.SourceReservePlan5,
      private val dropTag: IoOperationTag,
      private var metadataCharge: AutoCloseable
  ):
    private var issuerClaimed = false
    private var receiptIssued = false
    private var terminalAckSettled = false
    private var terminalAckFailure: Throwable = null

    private[floatingpod] def requireFor(actualLease: SourceContinuationLease,
        actualOverlay: SourceChunkEffectClosure,
        exactPlanAck: MySqlOwnedIoAdapter.RestrictedSourcePlanAck,
        exactPlan: MySqlFencedTransaction.SourceReservePlan5): Unit = {
      require(!issuerClaimed && (lease eq actualLease) && (overlay eq actualOverlay) &&
        (planAck eq exactPlanAck) && (plan eq exactPlan) && observation != null,
        "exact one-shot Main source-plan views-revoked proof required")
      require(overlay.isRevokedForDrop,
        "Main source-plan views were not revoked for this exact retained plan/carrier")
      issuerClaimed = true
    }

    /** 仅由 Fenced issuer 在留存此精确、不可变的 drop receipt 后调用。 */
    private[floatingpod] def recordReceiptIssued(
        exactReceipt: MySqlFencedTransaction.SourceReservePlanDropReceipt): Unit = {
      require(issuerClaimed && !receiptIssued && exactReceipt != null && planAck != null && plan != null &&
        metadataCharge != null,
        "exact Fenced source-plan drop receipt issuance required before revoking its Main payload anchors")
      exactReceipt.requireFor(plan)
      lease = null
      observation = null
      overlay = null
      planAck = null
      plan = null
      receiptIssued = true
    }

    /** 真实 worker ACK 只结算轻量 Main drop proof charge，不结算 provider barrier。 */
    private[floatingpod] def releaseAfterWorkerAck(
        exactAck: MySqlFencedTransaction.SourceReservePlanDropAck): Unit = {
      require(exactAck != null && receiptIssued,
        "exact terminal worker ACK required for Main source-plan drop proof settlement")
      if terminalAckSettled then return
      if terminalAckFailure != null then throw terminalAckFailure
      require(metadataCharge != null,
        "Main source-plan drop proof lost its outstanding metadata charge")
      if !terminalAckSettled then
        try metadataCharge.close()
        catch
          case cause: Throwable =>
            terminalAckFailure = cause
            throw cause
        metadataCharge = null
        terminalAckSettled = true
    }

    private[floatingpod] def tagForSubmit: IoOperationTag =
      require(dropTag != null && !terminalAckSettled,
        "current restricted source plan drop operation tag required")
      dropTag
    private[floatingpod] def isTerminalAckSettled: Boolean = terminalAckSettled

  /** Exact operation reservation installed before enqueue; outcomes remain with this original lease. */
  private[floatingpod] final class SourceSubmissionPending private[MainBootProjection] (
      private val lease: SourceContinuationLease,
      val tag: IoOperationTag,
      private val command: MySqlOwnedIoCommand,
      private val parent: MySqlNormalRecallRecoveryReference,
      val previousRepositoryEpoch: Long,
      val nextRepositoryEpoch: Long,
      private[MainBootProjection] val preparation:MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueue,
      private[MainBootProjection] val originalObservation:SourceContinuationObservation
  ):
    private[MainBootProjection] var submission: SourceContinuationLease.SubmissionState =
      SourceContinuationLease.SubmissionState.Begun
    private[MainBootProjection] def requireLease(actual: SourceContinuationLease): Unit =
      require((lease eq actual) && tag != null && command != null && parent != null &&
        originalObservation!=null && (preparation==null || (preparation.ownsMainPending(this) &&
          (preparation.expectedObservation eq originalObservation))),
        "exact original restricted source pending required")

  /** Exact retained Main owner for restricted source responsibility after the Boot DATA read retires. */
  private[floatingpod] final class SourceContinuationLease private[MainBootProjection] (
      private val projection: MainBootProjection,
      private val callbacks: ProductionDedicatedBootstrapCallbacks,
      private val server: MinecraftServer,
      private val originalReply: MySqlProductionBootReply,
      private val originalSnapshot: MySqlOwnedFormat3Snapshot,
      private[floatingpod] val link: MySqlOwnedIoAdapter.RecallRecoveryLink,
      private val root: com.wi.floatingpod.server.source.SourceForestAllocationBudget.Root,
      private val custody: NormalRecallRecoverySourceCustody,
      private val profile: SourceLimitProfile,
      private[floatingpod] val originalEpoch: Long,
      private val originalIdentity: MySqlOwnedIoIdentity,
      private val originalBinding: UUID,
      private val originalWriterEpoch: Long
  ):
    private[MainBootProjection] def belongsToProjection(actualProjection:MainBootProjection,
        actualCallbacks:ProductionDedicatedBootstrapCallbacks):Boolean =
      (projection eq actualProjection) && (callbacks eq actualCallbacks)

    private var registrationFailure: Throwable = null
    private var bootReadRetired = false
    private var currentObservation: SourceContinuationObservation = null
    private var pending: SourceSubmissionPending = null
    private var restrictedSourceR0Preparation:MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueue=null
    private var restrictedSourceR0HandoffFailureStage:
      MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage=null
    private var restrictedSourceR0HandoffFailure:Throwable=null
    private var sourceObservationBlocked = false
    private var lastSettledEpoch = originalEpoch
    private var originalProtectionIndex: MySqlBootSourceProtectionIndex = null
    private var originalProtectionData: MySqlBootSourceProtectionIndex.RetainedData = null
    private var originalProtectionBound = false
    private var sourceChunkEffectClosure: SourceChunkEffectClosure = null
    // Provider generation barrier 的寿命长于单次只读 worker plan drop。
    // HELD/DEFER 会标记其未完成，因此 worker ACK 不能释放该责任或对应 charge。
    private var sourceProviderBarrier:
      com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission#SourceScopedBarrier = null
    private var sourcePlanDropProof: SourcePlanViewsRevoked = null
    private var sourcePlanDropSubmission: MySqlRestrictedSourcePlanDropSubmission = null
    private var sourcePlanDropFailure: Throwable = null
    private var failedSourcePlanDropCharge: AutoCloseable = null
    private var completedSourcePlanDropTag: IoOperationTag = null
    private var completedSourcePlanDropAck: MySqlFencedTransaction.SourceReservePlanDropAck = null

    private[floatingpod] def retainRegistrationFailure(cause: Throwable): Unit =
      require(cause != null, "source continuation registration cause required")
      if registrationFailure == null then registrationFailure = cause
      else if registrationFailure ne cause then registrationFailure.addSuppressed(cause)

    /** Retains the fully copied/charged original source-protection view before Boot DATA retirement. */
    private[floatingpod] def retainProtectionData(exactCustody: NormalRecallRecoverySourceCustody,
        exactIndex: MySqlBootSourceProtectionIndex,
        exactData: MySqlBootSourceProtectionIndex.RetainedData, expectedEpoch: Long): Unit =
      require(exactData != null, "exact retained source-protection DATA holder required")
      if originalProtectionData == null then
        originalProtectionData = exactData
        originalProtectionIndex = exactIndex
      else
        if (originalProtectionData ne exactData) then
          val cause = new IllegalStateException("original Boot protection view is retained once by this exact continuation")
          retainRegistrationFailure(cause)
          throw cause
        if originalProtectionBound then
          require((exactCustody eq custody) && expectedEpoch == originalEpoch,
            "retained source-protection idempotent lookup must preserve its original custody/epoch")
          return
        if originalProtectionIndex ne exactIndex then
          val cause = new IllegalStateException("partial original Boot protection binding changed its exact index")
          retainRegistrationFailure(cause)
          throw cause
      if registrationFailure != null then throw registrationFailure
      try
        require(exactCustody eq custody, "retained source-protection holder belongs to the exact original custody")
        require(!bootReadRetired && pending == null && !sourceObservationBlocked && expectedEpoch == originalEpoch &&
          callbacks.adapter.repositorySubmissionEpoch == originalEpoch,
          "original Boot protection DATA must be retained before source submission or Boot retirement")
        projection.requireOriginalSourceProtectionIndex(exactIndex)
        requireResponsibilityOwner(server, link)
        originalProtectionIndex = null // The closeable Account copy now owns the complete classification DATA.
        originalProtectionBound = true
      catch
        case cause: Throwable =>
          retainRegistrationFailure(cause)
          throw cause

    /** Immutable copied source DATA; the registered lease, not the old Boot projection, owns it. */
    private[floatingpod] def sourceProtectionData(): MySqlBootSourceProtectionIndex.RetainedData =
      requireResponsibilityOwner(server, link)
      require(originalProtectionBound, "original source-protection DATA holder binding is incomplete")
      Option(originalProtectionData).getOrElse(
        throw new IllegalStateException("original source-protection DATA holder is not retained"))

    /** Ordinary-save classification uses only the retained full view and never the global action gate. */
    private[floatingpod] def classifyRetainedSourceProtectionForOrdinaryChunk(
        dimension: Int, chunkX: Int, chunkZ: Int): MySqlBootSourceProtectionIndex.Classification =
      requireResponsibilityOwner(server, link)
      sourceProtectionData().classify(dimension, chunkX, chunkZ)

    /** Begin only the exact source-scoped provider barrier; null from Admission means retry after the Main scope. */
    private[floatingpod] def tryBeginManagedSourceScopedBarrier():
        Option[com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission#SourceScopedBarrier] =
      requireResponsibilityOwner(server, link)
      if sourcePlanDropFailure != null then throw sourcePlanDropFailure
      val observation = Option(currentObservation).getOrElse(
        throw new IllegalStateException("exact current source observation required before scoped plan read"))
      if sourcePlanDropProof != null && !sourcePlanDropProof.isTerminalAckSettled then return None
      if sourcePlanDropProof != null then sourcePlanDropProof = null
      if sourceChunkEffectClosure != null then None
      else Option(callbacks.ordinarySaveAdmission().tryBeginSourceScopedBarrier(this, server, observation))

    /** Charge Admission responsibility through the original custody account, never a caller Account. */
    private[floatingpod] def retainAdmissionCharge(
        exactObservation: SourceContinuationObservation, bytes: Long, purpose: String): AutoCloseable =
      requireAdmissionChargeObservation(custody, exactObservation)
      require(bytes > 0L && purpose != null && purpose.nonEmpty,
        "positive original-account Admission reservation required")
      custody.retainAdmissionCharge(this, exactObservation, bytes, purpose)

    /** Custody's package-private allocator verifies exact owner/observation without refreshing action freshness. */
    private[floatingpod] def requireAdmissionChargeObservation(
        exactCustody: NormalRecallRecoverySourceCustody,
        exactObservation: SourceContinuationObservation): Unit =
      requireResponsibilityOwner(server, link)
      require((custody eq exactCustody) && exactObservation != null && (exactObservation.lease eq this),
        "exact original source lease/custody/observation required for Admission charge")

    /** A real Main ACK binds the one readonly Fenced plan closure to this unchanged source observation. */
    private[floatingpod] def retainRestrictedSourcePlanClosure(
        expectedSourceObservation: SourceContinuationObservation,
        exactDomain: com.wi.floatingpod.server.source.SourceDomainCapture.CatalogDomainObservation,
        exactCarrierLocator: SourceCarrierLocator,
        exactPlanAck: MySqlOwnedIoAdapter.RestrictedSourcePlanAck,
        exactPlan: MySqlFencedTransaction.SourceReservePlan5,
        exactBarrier: com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission#SourceScopedBarrier,
        exactClosure: MySqlFencedTransaction.SourceReserveEffectClosure5): SourceChunkEffectClosure =
      requireReadonlySourcePlanAckCurrent(expectedSourceObservation, exactDomain)
      require(exactCarrierLocator != null && exactPlanAck != null && exactPlan != null &&
        exactBarrier != null && exactClosure != null,
        "actual settled typed Fenced source plan ACK/barrier/closure required")
      require(sourcePlanDropProof == null || sourcePlanDropProof.isTerminalAckSettled,
        "restricted source plan drop is still pending or unresolved")
      sourcePlanDropProof = null
      val exactCarrier = exactCarrierLocator.requireRetainedFor(this, expectedSourceObservation, exactDomain)
      require((exactPlan.effectClosure() eq exactClosure) &&
        exactClosure.containsExactCarrier(exactCarrier),
        "readonly source plan ACK does not carry the exact retained Fenced closure for the selected carrier")
      require(sourceChunkEffectClosure == null,
        "only one restricted source effect closure may be retained for an unchanged source observation")
      val retained = new SourceChunkEffectClosure(this, expectedSourceObservation, exactDomain,
        exactCarrierLocator, exactPlanAck, exactPlan, exactBarrier, exactClosure)
      // Allocate the retained owner before the one-way barrier transfer. If allocation fails,
      // Adapter still owns the untransferred token and its exact failure path can retain it.
      exactBarrier.requireTransferToLease(this, expectedSourceObservation)
      require(sourceProviderBarrier == null || (sourceProviderBarrier eq exactBarrier),
        "only the original provider protection generation may back restricted source plans")
      sourceProviderBarrier = exactBarrier
      sourceChunkEffectClosure = retained
      retained

    /** 仅在此原始 lease 通过 Main owner 检查后生成 drop tag。 */
    private[MainBootProjection] def newSourcePlanDropTag(): IoOperationTag =
      require(Thread.currentThread() eq server.getServerThread,
        "restricted source plan drop tag requires the original claimed Main")
      val tag = IoOperationTag(originalIdentity.bootId, originalIdentity.bootGeneration,
        originalIdentity.connectionGeneration, UUID.randomUUID())
      originalIdentity.requireTag(tag)
      tag

    private def closeUnissuedSourcePlanDropCharge(charge: AutoCloseable, primary: Throwable): Unit =
      try charge.close()
      catch
        case cleanup: Throwable =>
          if cleanup ne primary then primary.addSuppressed(cleanup)
          failedSourcePlanDropCharge = charge
          sourcePlanDropFailure = primary
          sourceObservationBlocked = true

    /** 只撤销精确 Main overlay，再请求 Adapter 异步释放 worker 所有的 plan。 */
    private[floatingpod] def submitRestrictedSourcePlanDrop(): MySqlRestrictedSourcePlanDropSubmission =
      requireResponsibilityOwner(server, link)
      if failedSourcePlanDropCharge != null then throw sourcePlanDropFailure
      val existingProof = sourcePlanDropProof
      if existingProof != null then
        if sourcePlanDropFailure != null then throw sourcePlanDropFailure
        sourcePlanDropSubmission match
          case MySqlRestrictedSourcePlanDropSubmission.Submitted(IoSubmission.RejectedCapacity) =>
            val retried = callbacks.adapter.submitRestrictedSourcePlanDrop(existingProof.tagForSubmit, existingProof)
            sourcePlanDropSubmission = retried
            retried
          case _ => MySqlRestrictedSourcePlanDropSubmission.AlreadyPending
      else
        val observation = Option(currentObservation).getOrElse(
          throw new IllegalStateException("exact current observation required before source plan drop"))
        require(!bootReadRetired && pending == null && !sourceObservationBlocked &&
          observation.sourceCompletion.isEmpty && observation.settledRepositoryEpoch == originalEpoch &&
          lastSettledEpoch == originalEpoch,
          "readonly source plan drop requires the unchanged original observation and no pending business mutation")
        requireReadonlySourcePlanAckCurrent(observation, observation.domain)
        val exactOverlay = Option(sourceChunkEffectClosure).getOrElse(
          throw new IllegalStateException("exact active source-plan closure required before drop"))
        val exactBarrier = Option(sourceProviderBarrier).getOrElse(
          throw new IllegalStateException("original provider protection generation is required before plan drop"))
        exactOverlay.requireFor(this, observation)
        exactOverlay.requireNoActivePlanViewsForDrop(this, observation)
        val metadataCharge = retainAdmissionCharge(observation, 256L,
          "Main source plan views-revoked drop proof BEFORE allocation")
        try
          exactBarrier.requirePlanViewRevocationCurrent(this, observation)
          // guard、revoke 与 detach 必须相邻执行；其间不能插入 callback、异步让出或观测变更。
          require(sourceChunkEffectClosure eq exactOverlay,
            "source plan wrapper changed after its exact revocation guard")
        catch
          case cause: Throwable =>
            closeUnissuedSourcePlanDropCharge(metadataCharge, cause)
            throw cause
        val proof =
          try exactOverlay.revokeViewsForDrop(this, observation, metadataCharge)
          catch
            case cause: Throwable =>
              closeUnissuedSourcePlanDropCharge(metadataCharge, cause)
              throw cause
        sourcePlanDropProof = proof
        sourcePlanDropSubmission = null
        sourcePlanDropFailure = null
        sourceChunkEffectClosure = null
        sourceObservationBlocked = true
        try
          val submitted = callbacks.adapter.submitRestrictedSourcePlanDrop(proof.tagForSubmit, proof)
          sourcePlanDropSubmission = submitted
          submitted match
            case MySqlRestrictedSourcePlanDropSubmission.Submitted(IoSubmission.RejectedStopped(Some(cause))) =>
              retainSourcePlanDropFailure(callbacks.adapter, cause)
            case _ => ()
          submitted
        catch
          case cause: Throwable =>
            retainSourcePlanDropFailure(callbacks.adapter, cause)
            throw cause

    /** Adapter 在 poll 与后续真实 worker ACK 中复用同一精确 drop token/tag。 */
    private[floatingpod] def pollRestrictedSourcePlanDrop(max: Int): MySqlRestrictedSourcePlanDropPoll =
      requireResponsibilityOwner(server, link)
      if sourcePlanDropFailure != null then throw sourcePlanDropFailure
      require(sourcePlanDropProof != null && !sourcePlanDropProof.isTerminalAckSettled,
        "exact pending restricted source plan drop required before polling")
      if failedSourcePlanDropCharge != null then
        throw new IllegalStateException("failed source-plan drop charge cleanup retains the original responsibility",
          sourcePlanDropFailure)
      try
        val result = callbacks.adapter.pollRestrictedSourcePlanDrop(max)
        result match
          case MySqlRestrictedSourcePlanDropPoll.Unknown(cause) => retainSourcePlanDropFailure(callbacks.adapter, cause)
          case MySqlRestrictedSourcePlanDropPoll.Failed(cause) => retainSourcePlanDropFailure(callbacks.adapter, cause)
          case MySqlRestrictedSourcePlanDropPoll.Rejected(value) =>
            sourcePlanDropSubmission = MySqlRestrictedSourcePlanDropSubmission.Submitted(value)
            value match
              case IoSubmission.RejectedStopped(Some(cause)) => retainSourcePlanDropFailure(callbacks.adapter, cause)
              case _ => ()
          case _ => ()
        result
      catch
        case cause: Throwable =>
          retainSourcePlanDropFailure(callbacks.adapter, cause)
          throw cause

    private[floatingpod] def retainSourcePlanDropFailure(actualAdapter: MySqlOwnedIoAdapter,
        cause: Throwable): Unit =
      require((actualAdapter eq callbacks.adapter) && sourcePlanDropProof != null && cause != null,
        "exact original Adapter/source-plan drop responsibility and cause required")
      if sourcePlanDropFailure == null then sourcePlanDropFailure = cause
      else if sourcePlanDropFailure ne cause then sourcePlanDropFailure.addSuppressed(cause)
      sourceObservationBlocked = true

    /** 仅在原始 Main 上的精确 Adapter 观察到真实 Fenced worker ACK 后调用。 */
    private[floatingpod] def acknowledgeSourcePlanDrop(actualAdapter: MySqlOwnedIoAdapter,
        exactAck: MySqlFencedTransaction.SourceReservePlanDropAck): Unit =
      requireResponsibilityOwner(server, link)
      require(actualAdapter eq callbacks.adapter, "original Adapter required for source plan drop ACK")
      require(exactAck != null, "exact typed source plan drop worker ACK required")
      if completedSourcePlanDropAck eq exactAck then
        val completedTag = Option(completedSourcePlanDropTag).getOrElse(
          throw new IllegalStateException("completed source plan drop ACK lost its bounded local tag"))
        actualAdapter.requireCompletedRestrictedSourcePlanDropAckFor(this, completedTag, exactAck)
        return
      val proof = sourcePlanDropProof
      if proof == null then
        val completedTag = Option(completedSourcePlanDropTag).getOrElse(
          throw new IllegalStateException("exact pending source plan drop proof required for worker ACK"))
        require(completedSourcePlanDropAck eq exactAck,
          "duplicate source-plan drop ACK differs from the exact locally completed ACK identity")
        actualAdapter.requireCompletedRestrictedSourcePlanDropAckFor(this, completedTag, exactAck)
        return
      actualAdapter.requireRestrictedSourcePlanDropAckFor(this, proof, exactAck)
      val completedTag = proof.tagForSubmit
      try
        if !proof.isTerminalAckSettled then proof.releaseAfterWorkerAck(exactAck)
      catch
        case cause: Throwable =>
          retainSourcePlanDropFailure(actualAdapter, cause)
          throw cause
      completedSourcePlanDropTag = completedTag
      completedSourcePlanDropAck = exactAck
      sourcePlanDropProof = null
      sourcePlanDropSubmission = null
      sourcePlanDropFailure = null
      sourceObservationBlocked = false

    /** Adapter tombstone failure must retain the cause and block a successor plan after the true worker ACK. */
    private[floatingpod] def retainCompletedSourcePlanDropAdapterFailure(
        actualAdapter: MySqlOwnedIoAdapter,
        exactAck: MySqlFencedTransaction.SourceReservePlanDropAck,
        cause: Throwable): Unit = {
      require((actualAdapter eq callbacks.adapter) && sourcePlanDropProof == null &&
        completedSourcePlanDropTag != null && (completedSourcePlanDropAck eq exactAck) && cause != null,
        "exact completed source-plan drop ACK and Adapter failure cause required")
      if sourcePlanDropFailure == null then sourcePlanDropFailure = cause
      else if sourcePlanDropFailure ne cause then sourcePlanDropFailure.addSuppressed(cause)
      sourceObservationBlocked = true
    }

    /** Typed provider/event decision; the current DATA/readonly-plan APIs hold or defer until outside proof exists. */
    private[floatingpod] def decideProviderChunk(actualProvider: ChunkProviderServer,
        actualWorld: WorldServer, actualChunk: Chunk,
        operation: com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.ProviderOperation):
        com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.ProviderDecision =
      requireResponsibilityOwner(server, link)
      require(actualProvider != null && actualWorld != null && actualChunk != null && operation != null,
        "actual provider/world/chunk/operation required for source save decision")
      require((actualWorld.getMinecraftServer eq server) && actualWorld.provider != null &&
        (actualWorld.getChunkProvider eq actualProvider) && (actualChunk.getWorld eq actualWorld),
        "source save provider/world/chunk identity changed")
      val dimension = actualWorld.provider.getDimension
      val chunkX = actualChunk.x
      val chunkZ = actualChunk.z
      sourceProtectionData().classify(dimension, chunkX, chunkZ) match
        case MySqlBootSourceProtectionIndex.Unprotected =>
          if pending != null || sourceObservationBlocked then
            return com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.ProviderDecision.DEFER
          val observation = currentObservation
          val effect = sourceChunkEffectClosure
          if observation == null || effect == null ||
              callbacks.adapter.repositorySubmissionEpoch != observation.settledRepositoryEpoch ||
              observation.settledRepositoryEpoch != lastSettledEpoch then
            com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.ProviderDecision.DEFER
          else
            effect.requireFor(this, observation)
            val view = effect.borrowPlanView(this, observation)
            var primary: Throwable = null
            try
              if view.globalUnresolved || view.containsChunk(dimension, chunkX, chunkZ) then
                com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.ProviderDecision.HELD
              else
                // The current plan closure describes affected DATA, but no issuer proves the full outside view unchanged.
                com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.ProviderDecision.DEFER
            catch
              case cause: Throwable =>
                primary = cause
                throw cause
            finally
              if primary == null then view.close()
              else
                try view.close()
                catch
                  case cleanup: Throwable => if cleanup ne primary then primary.addSuppressed(cleanup)
        case _ => com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.ProviderDecision.HELD

    private def requireExactBinding(actualServer: MinecraftServer,
        actualLink: MySqlOwnedIoAdapter.RecallRecoveryLink): Unit =
      require((actualServer eq server) && (callbacks.actualServer eq server) &&
        (Thread.currentThread() eq server.getServerThread), "original claimed Main source continuation required")
      require((actualLink eq link) && (link.identity eq originalIdentity) &&
        link.identity.bootId == originalIdentity.bootId && link.identity.bootGeneration == originalIdentity.bootGeneration &&
        link.identity.connectionGeneration == originalIdentity.connectionGeneration &&
        link.bindingId == originalBinding && link.writerEpoch == originalWriterEpoch &&
        link.sourceAllocation.exists(_ eq root), "original source continuation link/root/session changed")
      require((callbacks.adapter.identity eq originalIdentity) &&
        originalIdentity.bootId == link.identity.bootId &&
        originalIdentity.bootGeneration == link.identity.bootGeneration &&
        originalIdentity.connectionGeneration == link.identity.connectionGeneration,
        "original adapter boot/connection generation changed")
      require(callbacks.worldContext.actualServer() eq server, "original source world context changed")

    /** Custody may bind this holder before its callbacks/Adapter responsibility route is published. */
    private[floatingpod] def requireOriginalCustodyBinding(actualServer: MinecraftServer,
        actualLink: MySqlOwnedIoAdapter.RecallRecoveryLink,
        exactCustody: NormalRecallRecoverySourceCustody): Unit =
      requireExactBinding(actualServer, actualLink)
      if registrationFailure != null then throw registrationFailure
      require((custody eq exactCustody) &&
        (projection.recallSourceCustody.exists(_ eq exactCustody)) &&
        (projection.recallSourceCustodyRoot eq root) &&
        originalReply != null && originalSnapshot != null &&
        (profile eq MySqlProductionBootReply.actualSourceLimitProfile(originalReply)) &&
        originalEpoch == originalSnapshot.requireOriginalBootEpoch(callbacks.bootTag),
        "exact original Main/server/link/root/custody/profile binding required")

    /** Allows the original registered Main responsibility to keep polling and settling retained work. */
    private[floatingpod] def requireResponsibilityOwner(actualServer: MinecraftServer,
        actualLink: MySqlOwnedIoAdapter.RecallRecoveryLink): Unit =
      requireExactBinding(actualServer, actualLink)
      if registrationFailure != null then throw registrationFailure
      projection.lifetime.requireResponsibilityOwner()
      callbacks.requireRegisteredRecallSourceContinuation(projection, this, server, originalReply, link, root, custody, profile)
      custody.requireAllocationResponsibility()
      // This authenticates the still-registered Adapter route but deliberately does not require current freshness;
      // stale/UNKNOWN work must remain pollable and cleanable by this original owner.
      callbacks.adapter.pollRecallMaintenance(link)

    /** First real END capture binds the initial source observation before any restricted submission. */
    private[floatingpod] def observeInitialSourceDomain(
        exactDomain: com.wi.floatingpod.server.source.SourceDomainCapture.CatalogDomainObservation): SourceContinuationObservation =
      requireResponsibilityOwner(server, link)
      require(!bootReadRetired && pending == null && currentObservation == null && !sourceObservationBlocked,
        "initial source observation requires the still-current original Boot and an empty continuation slot")
      projection.requireCurrentReads()
      val actualDomain = custody.currentDomainObservation()
      require(actualDomain eq exactDomain, "initial source observation differs from the captured custody domain")
      val observed = callbacks.adapter.pollRecallMaintenance(link)
      observed.failure.foreach(throw _)
      val freshness = observed.latest.getOrElse(
        throw new IllegalStateException("original restricted source maintenance observation missing"))
      require(observed.isCurrent && freshness.bootId == originalIdentity.bootId &&
        freshness.bootGeneration == originalIdentity.bootGeneration &&
        freshness.connectionGeneration == originalIdentity.connectionGeneration &&
        freshness.bindingId == originalBinding && freshness.writerEpoch == originalWriterEpoch &&
        freshness.profile == MySqlSchemaAuditProfile.DeploymentReceipt1,
        "original restricted source maintenance observation is stale or foreign")
      val actualEpoch = callbacks.adapter.repositorySubmissionEpoch
      require(actualEpoch == originalEpoch && actualEpoch == lastSettledEpoch,
        "source continuation initial epoch differs from the exact original Boot epoch")
      val result = new SourceContinuationObservation(this, exactDomain, freshness, actualEpoch, None)
      currentObservation = result
      result

    /** Strong action gate; a responsibility lease alone never authorizes capture, native IO, or SQL. */
    private[floatingpod] def requireActionCurrent(expectedSourceObservation: SourceContinuationObservation): Unit =
      requireResponsibilityOwner(server, link)
      require(expectedSourceObservation != null && (currentObservation eq expectedSourceObservation) &&
        (expectedSourceObservation.lease eq this), "exact current source observation required")
      require(!sourceObservationBlocked && pending == null,
        "restricted source action waits for the exact prior settlement and completion ACK")
      val currentDomain = custody.currentDomainObservation()
      require(currentDomain eq expectedSourceObservation.domain,
        "original captured source domain observation changed")
      val observed = callbacks.adapter.pollRecallMaintenance(link)
      observed.failure.foreach(throw _)
      val latest = observed.latest.getOrElse(
        throw new IllegalStateException("original restricted source maintenance observation missing"))
      require(observed.isCurrent && latest.isCurrent &&
        latest.bootId == originalIdentity.bootId && latest.bootGeneration == originalIdentity.bootGeneration &&
        latest.connectionGeneration == originalIdentity.connectionGeneration && latest.bindingId == originalBinding &&
        latest.writerEpoch == originalWriterEpoch && latest.profile == MySqlSchemaAuditProfile.DeploymentReceipt1,
        "restricted source current maintenance/fence family is stale or foreign")
      val checkpoint = callbacks.adapter.checkpoint.io
      checkpoint.fatalCause.foreach(throw _)
      checkpoint.exitFailure.foreach(throw _)
      require(checkpoint.accepting && !checkpoint.stopped, "original restricted source adapter is not accepting work")
      require(callbacks.adapter.repositorySubmissionEpoch == expectedSourceObservation.settledRepositoryEpoch &&
        expectedSourceObservation.settledRepositoryEpoch == lastSettledEpoch,
        "restricted source action does not match the exact settled repository epoch")
      callbacks.adapter.requireRestrictedSourceActionNoUnresolved(link, expectedSourceObservation.settledRepositoryEpoch)
      if bootReadRetired then
        val ack = expectedSourceObservation.sourceCompletion.getOrElse(
          throw new IllegalStateException("retired Boot DATA cannot authorize a new source action without an exact ACKed Full26 source completion"))
        ack.requireFor(this, expectedSourceObservation.settledRepositoryEpoch)
      else
        require(expectedSourceObservation.sourceCompletion.isEmpty &&
          expectedSourceObservation.settledRepositoryEpoch == originalEpoch,
          "initial action must use the exact original Boot source observation")

    /** 通过精确封闭的 R0 role 复核原 Main owner、parent 与 observation。 */
    private def requireRestrictedSourceR0IdentityFacts(
        exactPreparation:MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueue):Unit =
      requireResponsibilityOwner(server,link)
      require(exactPreparation!=null && (exactPreparation.lease eq this) &&
        (restrictedSourceR0Preparation eq exactPreparation),
        "exact Adapter-issued restricted SOURCE R0 preparation is retained by this Main lease")
      val observation=exactPreparation.expectedObservation
      exactPreparation.requireOriginal(callbacks.adapter,this,link,observation)
      require(custody.currentDomainObservation() eq observation.domain,
        "restricted SOURCE R0 captured Main domain changed before enqueue")

    /** P5 重核原 domain、maintenance/fence、checkpoint 与 settled epoch。 */
    private def requireRestrictedSourceR0ReadinessFacts(
        exactPreparation:MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueue):Unit =
      requireRestrictedSourceR0IdentityFacts(exactPreparation)
      val observation=exactPreparation.expectedObservation
      val observed=callbacks.adapter.pollRecallMaintenance(link)
      observed.failure match
        case Some(cause)=>throw cause
        case None=>()
      val latest=observed.latest match
        case Some(value)=>value
        case None=>throw new IllegalStateException("restricted SOURCE R0 requires current original maintenance evidence")
      require(observed.isCurrent && latest.isCurrent && latest.bootId==originalIdentity.bootId &&
        latest.bootGeneration==originalIdentity.bootGeneration &&
        latest.connectionGeneration==originalIdentity.connectionGeneration && latest.bindingId==originalBinding &&
        latest.writerEpoch==originalWriterEpoch && latest.profile==MySqlSchemaAuditProfile.DeploymentReceipt1,
        "restricted SOURCE R0 maintenance/fence family is stale or foreign")
      val checkpoint=callbacks.adapter.checkpoint.io
      checkpoint.fatalCause match
        case Some(cause)=>throw cause
        case None=>()
      checkpoint.exitFailure match
        case Some(cause)=>throw cause
        case None=>()
      require(checkpoint.accepting && !checkpoint.stopped,
        "original restricted SOURCE R0 Adapter is not accepting work")
      require(callbacks.adapter.repositorySubmissionEpoch==observation.settledRepositoryEpoch &&
        observation.settledRepositoryEpoch==lastSettledEpoch,
        "restricted SOURCE R0 differs from the exact settled Main/Adapter epoch")
      callbacks.adapter.requireRestrictedSourceR0PreEnqueue(exactPreparation,this,observation)

    /** 先安装两端精确 pending owner，再阻断并退役一次性原 Boot 读取。 */
    private[floatingpod] def registerRestrictedSourceR0Pending(
        exactPreparation:MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueue):SourceSubmissionPending =
      require(exactPreparation!=null && (currentObservation eq exactPreparation.expectedObservation) &&
        pending==null && restrictedSourceR0Preparation==null && !sourceObservationBlocked,
        "restricted SOURCE R0 Main registration requires the exact still-current unblocked observation")
      restrictedSourceR0Preparation=exactPreparation
      val observation=exactPreparation.expectedObservation
      requireRestrictedSourceR0IdentityFacts(exactPreparation)
      callbacks.adapter.requireRestrictedSourceR0PreEnqueue(exactPreparation,this,observation)
      if bootReadRetired then
        val ack=observation.sourceCompletion match
          case Some(value)=>value
          case None=>throw new IllegalStateException("previously retired Boot DATA requires its exact source-completion ACK")
        ack.requireFor(this,observation.settledRepositoryEpoch)
      else
        require(observation.sourceCompletion.isEmpty && observation.settledRepositoryEpoch==originalEpoch,
          "first restricted SOURCE R0 must use the exact original Boot observation")

      exactPreparation.beginMainPendingRegistration()
      exactPreparation.requireMainPendingBindAvailable()
      val value=new SourceSubmissionPending(this,exactPreparation.exactTag,exactPreparation.exactCommand,
        exactPreparation.exactParentReference,exactPreparation.expectedSourceEpoch,
        exactPreparation.nextAcceptedEpoch,exactPreparation,observation)
      exactPreparation.bindMainPending(value)
      require(pending==null,"original restricted source Main pending slot changed during exact registration")
      pending=value
      exactPreparation.confirmMainPendingInstalled(value)
      require((pending eq value) && exactPreparation.ownsMainPending(value) && value.preparation.eq(exactPreparation),
        "restricted SOURCE R0 Main and Adapter pending registrations must be bidirectional")

      sourceObservationBlocked=true
      currentObservation=null
      if bootReadRetired then
        exactPreparation.recordBootReadAlreadyRetired()
      else
        exactPreparation.markRetirementAttempted()
        projection.retireBootReadForSourceContinuation(this,exactPreparation.expectedSourceEpoch)
        bootReadRetired=true
        exactPreparation.markRetirementReturned()
      value

    /** P5 保留全部 pending owner，并在入队前完成最后一次 role 范围内复核。 */
    private[floatingpod] def requireRestrictedSourceR0EnqueueReady(
        exactPreparation:MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueue):Unit =
      require(exactPreparation!=null && (restrictedSourceR0Preparation eq exactPreparation) &&
        pending!=null && exactPreparation.ownsMainPending(pending) && exactPreparation.mainPendingBound &&
        sourceObservationBlocked && currentObservation==null,
        "restricted SOURCE R0 enqueue readiness requires both retained Main/Adapter pending owners")
      requireRestrictedSourceR0ReadinessFacts(exactPreparation)
      val observation=exactPreparation.expectedObservation
      if exactPreparation.bootReadRetiredByThisHandoff then
        require(bootReadRetired && observation.sourceCompletion.isEmpty,
          "this R0 handoff must retain the actual one-shot Boot retirement without inventing a completion ACK")
      else
        require(exactPreparation.previousBootReadAckVerified && bootReadRetired,
          "previously retired Boot DATA must have its exact ACK before another source action")
        val ack=observation.sourceCompletion match
          case Some(value)=>value
          case None=>throw new IllegalStateException("previously retired Boot DATA requires its exact source-completion ACK")
        ack.requireFor(this,observation.settledRepositoryEpoch)
      exactPreparation.markEnqueueReadinessCheck()

    /** 保留交接责任；不把它转换成 END 注册或 lifetime 失败。 */
    private[floatingpod] def retainRestrictedSourceR0HandoffFailure(
        exactPreparation:MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueue,
        stage:MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage,cause:Throwable):Unit =
      requireResponsibilityOwner(server,link)
      require(exactPreparation!=null && stage!=null && cause!=null &&
        (exactPreparation.adapter eq callbacks.adapter) && (exactPreparation.lease eq this) &&
        (exactPreparation.exactLink eq link),
        "exact Adapter/Main source handoff failure responsibility required")
      require((restrictedSourceR0Preparation==null || (restrictedSourceR0Preparation eq exactPreparation)) &&
        (pending==null || exactPreparation.ownsMainPending(pending)),
        "source handoff failure cannot replace a foreign Main preparation or pending")
      if restrictedSourceR0Preparation==null then restrictedSourceR0Preparation=exactPreparation
      sourceObservationBlocked=true
      if restrictedSourceR0HandoffFailure==null then
        restrictedSourceR0HandoffFailureStage=stage
        restrictedSourceR0HandoffFailure=cause

    /** Exact readonly-plan ACK check; its own source-plan pending slot is not an unrelated mutation. */
    private def requireReadonlySourcePlanAckCurrent(
        expectedSourceObservation: SourceContinuationObservation,
        exactDomain: com.wi.floatingpod.server.source.SourceDomainCapture.CatalogDomainObservation): Unit =
      requireResponsibilityOwner(server, link)
      require(expectedSourceObservation != null && (currentObservation eq expectedSourceObservation) &&
        (expectedSourceObservation.lease eq this) && exactDomain != null &&
        (expectedSourceObservation.domain eq exactDomain),
        "readonly source plan ACK requires the exact current observation and captured domain")
      require(!bootReadRetired && pending == null && !sourceObservationBlocked &&
        expectedSourceObservation.sourceCompletion.isEmpty &&
        expectedSourceObservation.settledRepositoryEpoch == originalEpoch &&
        expectedSourceObservation.settledRepositoryEpoch == lastSettledEpoch &&
        callbacks.adapter.repositorySubmissionEpoch == expectedSourceObservation.settledRepositoryEpoch,
        "readonly source plan ACK cannot outlive the original still-settled Boot epoch")
      require(custody.currentDomainObservation() eq exactDomain,
        "readonly source plan ACK differs from the exact loaded Main domain")
      val observed = callbacks.adapter.pollRecallMaintenance(link)
      observed.failure.foreach(throw _)
      val freshness = observed.latest.getOrElse(
        throw new IllegalStateException("readonly source plan ACK requires a current restricted maintenance observation"))
      require(observed.isCurrent && freshness.isCurrent &&
        freshness.bootId == originalIdentity.bootId && freshness.bootGeneration == originalIdentity.bootGeneration &&
        freshness.connectionGeneration == originalIdentity.connectionGeneration && freshness.bindingId == originalBinding &&
        freshness.writerEpoch == originalWriterEpoch && freshness.profile == MySqlSchemaAuditProfile.DeploymentReceipt1,
        "readonly source plan ACK has stale or foreign restricted maintenance evidence")
      val checkpoint = callbacks.adapter.checkpoint.io
      checkpoint.fatalCause.foreach(throw _)
      checkpoint.exitFailure.foreach(throw _)
      require(checkpoint.accepting && !checkpoint.stopped,
        "original restricted source adapter stopped before readonly plan ACK")

    /** Adapter planning may inspect only the domain object captured by this exact current lease. */
    private[floatingpod] def requireExactDomainObservation(
        expectedSourceObservation: SourceContinuationObservation,
        actualDomain: com.wi.floatingpod.server.source.SourceDomainCapture.CatalogDomainObservation): Unit =
      requireActionCurrent(expectedSourceObservation)
      require(actualDomain != null && (expectedSourceObservation.domain eq actualDomain),
        "restricted source plan requires the exact captured Main domain observation")

    /** Issues one locator only for the exact retained carrier selected by Main for its target chunk. */
    private[floatingpod] def sourceCarrierLocator(expectedSourceObservation: SourceContinuationObservation,
        actualDomain: com.wi.floatingpod.server.source.SourceDomainCapture.CatalogDomainObservation,
        dimension: Int, chunkX: Int, chunkZ: Int,
        exactCarrier: MySqlBootSourceProtectionIndex.Carrier): SourceCarrierLocator =
      requireExactDomainObservation(expectedSourceObservation, actualDomain)
      require(exactCarrier != null, "exact retained original carrier selection required")
      val holder = sourceProtectionData()
      var index = 0
      var exactIndex = -1
      while index < holder.carrierCount do
        val carrier = holder.carrierAt(index)
        if carrier eq exactCarrier then exactIndex = index
        index = Math.addExact(index, 1)
      require(exactIndex >= 0 && holder.containsExactCarrier(exactCarrier) &&
        exactCarrier.chunk.dimension == dimension && exactCarrier.chunk.x == chunkX && exactCarrier.chunk.z == chunkZ,
        "selected carrier must be the exact retained holder member at the Main-selected chunk")
      new SourceCarrierLocator(this, expectedSourceObservation, holder, exactIndex,
        dimension, chunkX, chunkZ, exactCarrier)

    /** Must run after the adapter reserved its exact completion slot and before any possible enqueue. */
    private[floatingpod] def beginSourceSubmission(exactTag: IoOperationTag,
        exactCommand: MySqlOwnedIoCommand, exactParent: MySqlNormalRecallRecoveryReference,
        expectedEpoch: Long): SourceSubmissionPending =
      val observation = currentObservation
      requireActionCurrent(observation)
      require(exactTag != null && exactCommand != null && exactParent != null,
        "exact restricted source command/tag/parent required")
      originalIdentity.requireTag(exactTag)
      require(link.references.exists(_ eq exactParent), "source parent is not the original linked reference")
      require(expectedEpoch == observation.settledRepositoryEpoch && expectedEpoch == lastSettledEpoch,
        "restricted source submission expected a foreign/unsettled epoch")
      val next = Math.addExact(expectedEpoch, 1L)
      val value = new SourceSubmissionPending(this, exactTag, exactCommand, exactParent, expectedEpoch, next,
        null,observation)
      require(pending == null, "original restricted source submission slot is already occupied")
      if !bootReadRetired then
        projection.retireBootReadForSourceContinuation(this, expectedEpoch)
        bootReadRetired = true
      pending = value
      currentObservation = null
      sourceObservationBlocked = true
      value

    /** Only the exact Adapter may report the real result returned by its queue submission call. */
    private[floatingpod] def recordSubmissionOutcome(actualAdapter: MySqlOwnedIoAdapter,
        originalPending: SourceSubmissionPending, result: IoSubmission): Unit =
      requireResponsibilityOwner(server, link)
      require((actualAdapter eq callbacks.adapter) && originalPending != null &&
        (pending eq originalPending) && originalPending.preparation==null,
        "only an original generic source pending may use this strict result entry")
      originalPending.requireLease(this)
      require(result != null && originalPending.submission == SourceContinuationLease.SubmissionState.Begun,
        "source submission outcome is recorded once")
      result match
        case IoSubmission.Accepted =>
          require(actualAdapter.repositorySubmissionEpoch == originalPending.nextRepositoryEpoch,
            "accepted restricted source submission did not advance exactly one repository epoch")
          originalPending.submission = SourceContinuationLease.SubmissionState.Accepted
        case rejected =>
          require(actualAdapter.repositorySubmissionEpoch == originalPending.previousRepositoryEpoch,
            "rejected restricted source submission changed repository epoch")
          originalPending.submission = SourceContinuationLease.SubmissionState.Rejected(rejected)
          sourceObservationBlocked = true

    /** Ambiguous enqueue keeps the original operation and cause; it never guesses the accepted epoch. */
    private[floatingpod] def recordSubmissionUnknown(actualAdapter: MySqlOwnedIoAdapter,
        originalPending: SourceSubmissionPending, cause: Throwable): Unit =
      requireResponsibilityOwner(server, link)
      require((actualAdapter eq callbacks.adapter) && originalPending != null &&
        (pending eq originalPending) && originalPending.preparation==null && cause != null,
        "only an original generic source pending may use this strict UNKNOWN entry")
      originalPending.requireLease(this)
      require(originalPending.submission == SourceContinuationLease.SubmissionState.Begun,
        "source submission outcome is recorded once")
      originalPending.submission = SourceContinuationLease.SubmissionState.SubmissionUnknown(cause)
      sourceObservationBlocked = true

    /** 仅精确封闭的 Adapter role 可记录其一次 OwnedSerialIo.submit 返回的真实结果。 */
    private[floatingpod] def recordSubmissionOutcome(actualAdapter:MySqlOwnedIoAdapter,
        originalPending:SourceSubmissionPending,
        exactPreparation:MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueue,result:IoSubmission):Unit =
      requireResponsibilityOwner(server,link)
      require(exactPreparation!=null && (restrictedSourceR0Preparation eq exactPreparation) &&
        (exactPreparation.adapter eq callbacks.adapter) && (exactPreparation.lease eq this) &&
        (actualAdapter eq callbacks.adapter) && originalPending!=null && (pending eq originalPending) &&
        (originalPending.preparation eq exactPreparation) && exactPreparation.ownsMainPending(originalPending) &&
        exactPreparation.mainPendingBound && exactPreparation.submissionReturned &&
        exactPreparation.adapterEpochFactRecorded && exactPreparation.pendingOutcomeFactRecorded &&
        actualAdapter.repositorySubmissionEpoch==exactPreparation.adapterEpochAtOutcomeObservation &&
        exactPreparation.rawSubmission==result &&
        exactPreparation.phase==MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.MainOutcomeRecording,
        "known R0 result must use the exact bidirectional Adapter/Main pending handoff")
      originalPending.requireLease(this)
      require(result!=null && originalPending.submission==SourceContinuationLease.SubmissionState.Begun,
        "restricted SOURCE R0 Main outcome is recorded once")
      result match
        case IoSubmission.Accepted=>
          require(actualAdapter.repositorySubmissionEpoch==originalPending.nextRepositoryEpoch,
            "known Accepted SOURCE R0 must retain its exact next Adapter epoch")
          originalPending.submission=SourceContinuationLease.SubmissionState.Accepted
        case rejected=>
          require(actualAdapter.repositorySubmissionEpoch==originalPending.previousRepositoryEpoch,
            "known rejected SOURCE R0 must retain its exact old Adapter epoch")
          originalPending.submission=SourceContinuationLease.SubmissionState.Rejected(rejected)
          sourceObservationBlocked=true

    /** 只有进入 submit 后抛出才是 UNKNOWN；此处不推测 epoch。 */
    private[floatingpod] def recordSubmissionUnknown(actualAdapter:MySqlOwnedIoAdapter,
        originalPending:SourceSubmissionPending,
        exactPreparation:MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueue,cause:Throwable):Unit =
      requireResponsibilityOwner(server,link)
      require(exactPreparation!=null && (restrictedSourceR0Preparation eq exactPreparation) &&
        (exactPreparation.adapter eq callbacks.adapter) && (exactPreparation.lease eq this) &&
        (actualAdapter eq callbacks.adapter) && originalPending!=null && (pending eq originalPending) &&
        (originalPending.preparation eq exactPreparation) && exactPreparation.ownsMainPending(originalPending) &&
        exactPreparation.mainPendingBound && exactPreparation.submitCallEntered &&
        !exactPreparation.submissionReturned && exactPreparation.adapterEpochFactRecorded &&
        exactPreparation.pendingOutcomeFactRecorded &&
        actualAdapter.repositorySubmissionEpoch==exactPreparation.adapterEpochAtOutcomeObservation &&
        (exactPreparation.submissionUnknownCause eq cause) && cause!=null,
        "UNKNOWN requires the exact submit-entered R0 pending and original thrown cause")
      originalPending.requireLease(this)
      require(originalPending.submission==SourceContinuationLease.SubmissionState.Begun,
        "restricted SOURCE R0 UNKNOWN is recorded once")
      originalPending.submission=SourceContinuationLease.SubmissionState.SubmissionUnknown(cause)
      sourceObservationBlocked=true

    private[floatingpod] def requireOriginal(actualProjection: MainBootProjection,
        actualCallbacks: ProductionDedicatedBootstrapCallbacks, actualServer: MinecraftServer,
        actualReply: MySqlProductionBootReply, actualLink: MySqlOwnedIoAdapter.RecallRecoveryLink,
        actualRoot: com.wi.floatingpod.server.source.SourceForestAllocationBudget.Root,
        actualCustody: NormalRecallRecoverySourceCustody, actualProfile: SourceLimitProfile): Unit =
      require((projection eq actualProjection) && (callbacks eq actualCallbacks) &&
        (server eq actualServer) && (originalReply eq actualReply) && (link eq actualLink) &&
        (root eq actualRoot) && (custody eq actualCustody) && (profile eq actualProfile),
        "original source continuation holder identities changed")

  private[floatingpod] object SourceContinuationLease:
    enum SubmissionState:
      case Begun
      case Accepted
      case Rejected(outcome: IoSubmission)
      case SubmissionUnknown(cause: Throwable)

    def retain(projection: MainBootProjection, callbacks: ProductionDedicatedBootstrapCallbacks,
        server: MinecraftServer, reply: MySqlProductionBootReply, snapshot: MySqlOwnedFormat3Snapshot,
        link: MySqlOwnedIoAdapter.RecallRecoveryLink,
        root: com.wi.floatingpod.server.source.SourceForestAllocationBudget.Root,
        custody: NormalRecallRecoverySourceCustody, profile: SourceLimitProfile,
        originalEpoch: Long): SourceContinuationLease =
      require(projection != null && callbacks != null && server != null && reply != null && snapshot != null &&
        link != null && root != null && custody != null && profile != null && originalEpoch >= 0L,
        "exact original restricted source continuation inputs required")
      val (bootId, bootGeneration, connectionGeneration, bindingId, writerEpoch) =
        MySqlProductionBootReply.sessionIdentity(reply)
      require((callbacks.actualServer eq server) && (projection.callbacks eq callbacks) &&
        (callbacks.adapter.identity eq link.identity) && (snapshot eq projection.snapshotInput) &&
        bootId == link.identity.bootId && bootGeneration == link.identity.bootGeneration &&
        connectionGeneration == link.identity.connectionGeneration && bindingId == link.bindingId &&
        writerEpoch == link.writerEpoch && snapshot.requireOriginalBootEpoch(callbacks.bootTag) == originalEpoch &&
        link.sourceAllocation.contains(root) && (profile eq MySqlProductionBootReply.actualSourceLimitProfile(reply)) &&
        (custody eq projection.recallSourceCustody.getOrElse(
          throw new IllegalStateException("original restricted source custody was not retained"))),
        "source continuation differs from exact original Boot/reply/link/root/profile/custody")
      new SourceContinuationLease(projection, callbacks, server, reply, snapshot, link, root, custody,
        profile, originalEpoch, link.identity, link.bindingId, link.writerEpoch)

  /** AboutToStart 的实际入口；Installation 原 holder 和 callbacks 两层消费均一次。 */
  def consumeClaimed(server: MinecraftServer): MainBootProjection =
    require(server != null && (Thread.currentThread() eq server.getServerThread), "actual dedicated Main required")
    ProductionBootstrapInstallation.consumeClaimedCallbacks(server) match
      case original: ProductionDedicatedBootstrapCallbacks => original.consumeClaimedMainProjection(server)
      case _ => throw new IllegalStateException("actual production callbacks required for Boot read input")
  private[floatingpod] def consumeOriginal(callbacks: ProductionDedicatedBootstrapCallbacks,
      server: MinecraftServer): MainBootProjection =
    require(callbacks != null && (server eq callbacks.actualServer), "original callbacks instance required")
    val originalReply = callbacks.consumeClaimedPreparation(server)
    callbacks.borrowClaimedProjection(server, originalReply)
  /** 只借用原 reply；AboutToStart 的一次消费仍在 consumeOriginal。 */
  private[floatingpod] def borrowOriginal(callbacks: ProductionDedicatedBootstrapCallbacks,
      server: MinecraftServer, reply: MySqlProductionBootReply): MainBootProjection =
    require(callbacks != null && (server eq callbacks.actualServer), "original callbacks instance required")
    val snapshot = MySqlProductionBootReply.fullTwentyRelationSnapshot(reply)
    val (bootId, bootGeneration, connectionGeneration, _, writerEpoch) = MySqlProductionBootReply.sessionIdentity(reply)
    val profile = MySqlProductionBootReply.actualSourceLimitProfile(reply)
    require(snapshot != null && (snapshot.limitProfile eq profile), "actual Boot profile reference required")
    val identity = callbacks.adapter.identity
    require(snapshot.bootUuid == identity.bootId && snapshot.bootGeneration == identity.bootGeneration &&
      snapshot.connectionGeneration == identity.connectionGeneration &&
      bootId == identity.bootId && bootGeneration == identity.bootGeneration &&
      connectionGeneration == identity.connectionGeneration,
      "original Boot/session generation required")
    reply match
      case recovering:MySqlOwnedIoReply.DeploymentReceiptRecallRecoveringBooted =>
        require(callbacks.approvedMode==com.wi.floatingpod.bootstrap.ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED &&
          (recovering.owned.identity eq identity) && recovering.owned.repositoryEpochAtSubmission==snapshot.requireOriginalBootEpoch(callbacks.bootTag),
          "restricted schema5 Boot requires original worker/mode/submission")
        snapshot.requireCompleteFamily()
      case deployment: MySqlOwnedIoReply.DeploymentReceiptBooted =>
        val expectedMode=deployment.owned.phase match
          case MySqlProductionBootReply.DeploymentReceiptPhase.FreshCertifiedPreparation =>
            com.wi.floatingpod.bootstrap.ProductionBootstrapControl.Mode.FRESH
          case MySqlProductionBootReply.DeploymentReceiptPhase.OrdinaryAdmitted =>
            com.wi.floatingpod.bootstrap.ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED
        require(callbacks.approvedMode==expectedMode && (deployment.owned.identity eq identity) &&
          deployment.owned.repositoryEpochAtSubmission==snapshot.requireOriginalBootEpoch(callbacks.bootTag),
          "schema5 Boot reply must retain original worker identity/mode/submission epoch")
        snapshot.requireCompleteFamily()
      case _: MySqlOwnedIoReply.FreshBooted =>
        require(callbacks.approvedMode == com.wi.floatingpod.bootstrap.ProductionBootstrapControl.Mode.FRESH,
          "Fresh Boot reply must match the exact approved mode")
      case _: MySqlOwnedIoReply.OrdinaryCertifiedBooted | _: MySqlOwnedIoReply.RecallRecoveringBooted =>
        val facts = MySqlProductionBootReply.ordinaryFacts(reply).get
        val approved = callbacks.approvedOriginalCreation
        require(callbacks.approvedMode == com.wi.floatingpod.bootstrap.ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED &&
          (facts.originalCreation eq approved) && facts.currentBootUuid == identity.bootId &&
          facts.currentBootGeneration == identity.bootGeneration && facts.connectionGeneration == identity.connectionGeneration &&
          facts.writerEpoch == writerEpoch && facts.sourceWorldIdentity == approved.sourceWorldIdentity() &&
          facts.guardFileIdentity == approved.guardFileIdentity() && facts.receiptFileIdentity == approved.receiptFileIdentity() &&
          facts.anchorFileIdentity == approved.anchorFileIdentity() &&
          facts.actualReceiptSha256.equalsIgnoreCase(approved.canonicalReceiptSha256()) &&
          facts.actualAnchorSha256.equalsIgnoreCase(approved.anchorSha256()) && facts.recordFacts.nonEmpty,
          "ordinary Boot reply must retain the exact approved birth source and actual current session/continuation facts")
    val result = new MainBootProjection(callbacks, reply, snapshot.requireBootReadData,
      MainBootReadLifetime.fromOriginalSource(Thread.currentThread(), snapshot, callbacks.bootTag,
        callbacks.adapter.repositorySubmissionEpoch))
    result.requireCurrentReads()
    result

/** 新正式 factory 可消费的完整读侧装配；没有 mutation facade 或 SQLite fallback。 */
final class MainBootReadConsumers private (
    val projection: MainBootProjection,
    val ownership: OwnershipValidator,
    val capacity: com.wi.floatingpod.network.ActiveCapacityPolicy,
    val entitlement: com.wi.floatingpod.network.PersistedPlayerEntitlementPolicy
)
object MainBootReadConsumers:
  private[floatingpod] def consumeClaimed(server: MinecraftServer, manager: ActiveFloatingPodManager): MainBootReadConsumers =
    require(manager != null, "actual Main manager required")
    val projection = MainBootProjection.consumeClaimed(server)
    try
      val ownership = OwnershipValidator.fromMainBoot(projection)
      val capacity = com.wi.floatingpod.network.ActiveCapacityPolicy.fromMainBoot(projection)
      val entitlement = com.wi.floatingpod.network.PersistedPlayerEntitlementPolicy.fromMainBoot(projection)
      manager.installMainBootReads(projection)
      new MainBootReadConsumers(projection, ownership, capacity, entitlement)
    catch
      case cause: Throwable => projection.retainConsumerFailure(cause)
