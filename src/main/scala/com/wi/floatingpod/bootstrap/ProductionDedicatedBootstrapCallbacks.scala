package com.wi.floatingpod.bootstrap

import com.wi.floatingpod.server.{DedicatedWorldContext, NormalRecallRecoverySourceCustody, WindowsB1SourceAnchorStore, WindowsWorldProvisioner}
import com.wi.floatingpod.config.MySqlRuntimeMetadata
import com.wi.floatingpod.server.io.{IoOperationTag, IoOwnerTransfer}
import com.wi.floatingpod.storage.mysql.*
import net.minecraft.server.MinecraftServer
import net.minecraft.world.WorldServer
import net.minecraft.world.chunk.Chunk
import com.wi.floatingpod.entity.FloatingPodEntity
import com.wi.floatingpod.server.MainBootProjection
import java.time.Duration
import java.util.IdentityHashMap
import java.util.concurrent.{CompletableFuture, CompletionStage, TimeUnit, TimeoutException}
import com.wi.sqljdbcdriverlib.diagnostics.SqlJdbcException
import scala.jdk.CollectionConverters.*

/** exact control/Main holder；worker只收到immutable命令，没有server/converter或用户closure。 */
final class ProductionDedicatedBootstrapCallbacks private (
    val actualServer: MinecraftServer, val control: DedicatedBootstrapBridge,
    originInput: ProductionDedicatedBootstrapCallbacks.Origin, val worldContext: DedicatedWorldContext,
    val dependencies: EarlyDependencyResolver.VerifiedDependencies,
    val adapter: MySqlOwnedIoAdapter, val bootTag: IoOperationTag,
    preparationInput: MySqlOwnedIoAdapter#BootstrapPreparation
) extends ManagedBootSaveCallbacks:
  private var retiredDeploymentMode:ProductionBootstrapControl.Mode=null
  private var retiredDeploymentLimit:Duration=null
  private var origin: ProductionDedicatedBootstrapCallbacks.Origin | MySqlOwnedIoAdapter.OrdinaryBootResultBacking = originInput
  private var preparationBacking: MySqlOwnedIoAdapter#BootstrapPreparation = preparationInput
  def preparation: adapter.BootstrapPreparation =
    require(preparationBacking != null, "bootstrap application preparation retired")
    preparationBacking.asInstanceOf[adapter.BootstrapPreparation]
  /** 原legacy调用保留；V3不能伪装成旧metadata/control。 */
  def approvedControl: ProductionBootstrapControl = origin match
    case ProductionDedicatedBootstrapCallbacks.Origin.Legacy(value) => value
    case ProductionDedicatedBootstrapCallbacks.Origin.DeploymentReceipt(_) => throw new IllegalStateException("schema5 has no legacy approved control")
    case ProductionDedicatedBootstrapCallbacks.Origin.V3(_) => throw new IllegalStateException("V3 has no legacy approved control")
    case _: MySqlOwnedIoAdapter.OrdinaryBootResultBacking => throw new IllegalStateException("application control retired")
    case null=>throw new IllegalStateException("schema5 application control retired")
  def approvedMode: ProductionBootstrapControl.Mode = origin match
    case ProductionDedicatedBootstrapCallbacks.Origin.Legacy(value) => value.mode()
    case ProductionDedicatedBootstrapCallbacks.Origin.V3(value) => value.mode()
    case ProductionDedicatedBootstrapCallbacks.Origin.DeploymentReceipt(value) => value.mode()
    case backing: MySqlOwnedIoAdapter.OrdinaryBootResultBacking => backing.retainedMode
    case null=>require(retiredDeploymentMode!=null,"original schema5 application retirement missing");retiredDeploymentMode
  def approvedActiveLimit: Int = origin match
    case ProductionDedicatedBootstrapCallbacks.Origin.Legacy(value) => value.metadata().activeLimit()
    case ProductionDedicatedBootstrapCallbacks.Origin.V3(value) => value.metadata().activeLimit()
    case ProductionDedicatedBootstrapCallbacks.Origin.DeploymentReceipt(value) => value.metadata().deploymentSemantics().canonicalFields().get("activeLimit").toInt
    case _: MySqlOwnedIoAdapter.OrdinaryBootResultBacking => throw new IllegalStateException("application active-limit input retired")
    case null=>throw new IllegalStateException("schema5 application active-limit input retired")
  def approvedOriginalCreation: ProductionOrdinarySourceApproval.ApprovedOriginalCreation = origin match
    case ProductionDedicatedBootstrapCallbacks.Origin.Legacy(value) => value.originalCreation()
    case ProductionDedicatedBootstrapCallbacks.Origin.V3(value) => value.originalCreation()
    case ProductionDedicatedBootstrapCallbacks.Origin.DeploymentReceipt(_) => throw new IllegalStateException("schema5 source pins are not legacy creation approval")
    case _: MySqlOwnedIoAdapter.OrdinaryBootResultBacking => throw new IllegalStateException("application creation input retired")
    case null=>throw new IllegalStateException("schema5 application creation input retired")
  private def approvedPolicy: ProductionBootstrapControl.Policy = origin match
    case ProductionDedicatedBootstrapCallbacks.Origin.Legacy(value) => value.policy()
    case ProductionDedicatedBootstrapCallbacks.Origin.V3(value) => value.policy()
    case ProductionDedicatedBootstrapCallbacks.Origin.DeploymentReceipt(value) => value.policy()
    case _: MySqlOwnedIoAdapter.OrdinaryBootResultBacking => throw new IllegalStateException("application policy retired")
    case null=>throw new IllegalStateException("schema5 application policy retired")
  private def approvedFinalizationLimit: Duration = origin match
    case backing: MySqlOwnedIoAdapter.OrdinaryBootResultBacking => backing.retainedFinalizationLimit
    case null=>require(retiredDeploymentLimit!=null,"original schema5 application retirement policy missing");retiredDeploymentLimit
    case _ => approvedPolicy.limits().finalization()
  private val exactControl = Thread.currentThread()
  @volatile private var reply: MySqlProductionBootReply = null
  @volatile private var originalReceipt: IoOwnerTransfer = null
  @volatile private var claimedMain: Thread = null
  @volatile private var failureReport: DedicatedBootstrapBridge.FailureReport = null
  @volatile private var finalCheckpoint: MySqlOwnedIoCheckpoint = null
  @volatile private var finalPreparation: Option[IoPreparationObservation] = None
  private var prepared = false
  private var claimedPreparationConsumed = false
  private def owner(): Unit = require(Thread.currentThread() eq exactControl, "exact boot-control owner required")
  private def requireReply(value: Object): Unit = require(reply != null && (value eq reply), "original preparation reply required")
  private def requireReceipt(value: Object): IoOwnerTransfer =
    require(originalReceipt != null && (value eq originalReceipt), "original opaque transfer receipt required")
    originalReceipt
  override def prepare(): CompletionStage[Object] =
    owner(); require(!prepared, "production preparation is one-shot"); prepared = true
    preparation.prepare(approvedPolicy.limits().preparation()).thenApply[Object] { value =>
      value match
        case deployment: MySqlOwnedIoReply.DeploymentReceiptBooted =>
          origin match
            case ProductionDedicatedBootstrapCallbacks.Origin.DeploymentReceipt(_) => ()
            case _ => throw new IllegalStateException("schema5 completion requires the original schema5 control")
          val backing=deployment.owned
          require(backing!=null && (backing.identity eq adapter.identity) && backing.actualSourceLimitProfile!=null,
            "original schema5 worker completion identity/profile required")
          backing.phase match
            case MySqlProductionBootReply.DeploymentReceiptPhase.FreshCertifiedPreparation =>
              require(approvedMode==ProductionBootstrapControl.Mode.FRESH,"schema5 Fresh preparation differs from approved mode")
            case MySqlProductionBootReply.DeploymentReceiptPhase.OrdinaryAdmitted =>
              require(approvedMode==ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED,"schema5 Ordinary completion differs from approved mode")
          backing.completeSnapshot.requireCompleteFamily()
          reply=deployment
          deployment
        case fresh: MySqlOwnedIoReply.FreshBooted =>
          require(approvedMode == ProductionBootstrapControl.Mode.FRESH,
            "Fresh completion does not match the approved production mode")
          require(fresh.actualSourceLimitProfile != null, "actual Fresh worker limit profile is required")
          reply = fresh
          fresh
        case ordinary: MySqlOwnedIoReply.OrdinaryCertifiedBooted =>
          require(approvedMode == ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED,
            "ordinary completion does not match the approved production mode")
          require(ordinary.owned.actualSourceLimitProfile != null &&
            (ordinary.owned.ordinaryFacts.originalCreation eq approvedOriginalCreation),
            "actual ordinary source facts must retain the approved original source reference")
          reply = ordinary
          ordinary
        case recovering:MySqlOwnedIoReply.DeploymentReceiptRecallRecoveringBooted =>
          val control=origin match
            case ProductionDedicatedBootstrapCallbacks.Origin.DeploymentReceipt(value)=>value
            case _=>throw new IllegalStateException("restricted schema5 Boot requires its actual control family")
          val backing=recovering.owned
          require(approvedMode==ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED &&
            (backing.identity eq adapter.identity) && (backing.originalSourcePins eq control.originalSourcePins()) &&
            (backing.actualSourceLimitProfile eq control.sourceLimitProfile()) &&
            backing.repositoryEpochAtSubmission==backing.completeSnapshot.requireOriginalBootEpoch(bootTag),
            "restricted schema5 Boot must retain original approved source/worker/profile/submission")
          backing.completeSnapshot.requireCompleteFamily()
          reply=recovering
          recovering
        case recovering: MySqlOwnedIoReply.RecallRecoveringBooted =>
          require(approvedMode == ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED &&
            recovering.actualSourceLimitProfile != null &&
            (recovering.ordinaryFacts.originalCreation eq approvedOriginalCreation),
            "restricted Recall must retain the actual approved Ordinary source")
          reply = recovering
          recovering
        case _ => throw new IllegalStateException("Actual production preparation returned another reply")
    }
  override def issueTransfer(value: Object, target: Thread): Object =
    owner(); requireReply(value)
    require(target != null && (target eq actualServer.getServerThread) && target.getState == Thread.State.NEW,
      "actual original NEW serverThread required")
    worldContext.pinBeforeOriginalStart(actualServer)
    val receipt = adapter.issueOwnerTransfer(target, bootTag)
    originalReceipt = receipt
    receipt
  override def claim(value: Object, receipt: Object): Unit =
    requireReply(value)
    require(Thread.currentThread() eq actualServer.getServerThread, "exact original server Main required")
    adapter.claimOwnerTransfer(requireReceipt(receipt)) // real consumption; no SQL/native wait or extra publication.
    claimedMain = Thread.currentThread()
  /** Installation calls only after bridge.claimTarget returns with Phase.CLAIMED; this issues no save permit. */
  def verifyClosedBootSaveScopeIssuer(server: MinecraftServer): Unit =
    require((server eq actualServer) && claimedMain != null &&
      (Thread.currentThread() eq claimedMain) && (claimedMain eq actualServer.getServerThread),
      "exact successfully claimed server Main required for closed save scope")
    worldContext.validateAboutToStart(server)
    val borrowed = borrowClaimedProjection(server, reply)
    borrowed.requireCurrentReads()
    // Retain the original schema5 source custody/account before the first Anvil loader can escape.
    // The custody itself captures its domain only on the first real END responsibility tick.
    closedScopeRecallSourceCustody = borrowed.retainRecallSourceCustody(server)
    closedScopeRecallSourceCustodyAttempted = true
    borrowed.retainAvailableRecallSourceContinuation(server)
    require(ordinarySaveOwner == null, "save owner is issued once")
    // 512 是 live bookkeeping 的保守逻辑容量 charge，不是 JVM heap 测量或原 SQL allocation ledger 预留。
    // 不复制 NBT；payload 仍为原 vanilla queue 引用，已完成 token 不保留永久历史。
    val maximumLive = Math.toIntExact(math.min(Int.MaxValue.toLong,
      MySqlProductionBootReply.actualSourceLimitProfile(reply).maxRetainedBytes() / 512L))
    ordinarySaveOwner = new com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission(claimedMain, maximumLive)
    adapter.installOrdinarySaveAdmission(ordinarySaveOwner)
  private var ordinarySaveOwner: com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission = null
  private var closedScopeRecallSourceCustodyAttempted = false
  private var closedScopeRecallSourceCustody = Option.empty[NormalRecallRecoverySourceCustody]
  private var recallSourceContinuationLease = Option.empty[MainBootProjection.SourceContinuationLease]
  private var borrowedProjection = Option.empty[com.wi.floatingpod.server.MainBootProjection]
  /** The original run-HEAD scope may bind only the custody/account preinitialized above. */
  private[floatingpod] def retainedRecallSourceCustodyForClosedScope(server: MinecraftServer): Option[NormalRecallRecoverySourceCustody] =
    verifyClosedBootSaveScopeUse(server)
    require(closedScopeRecallSourceCustodyAttempted,
      "original run-HEAD closed-scope source custody was not retained")
    closedScopeRecallSourceCustody.foreach(_.requireAllocationResponsibility())
    closedScopeRecallSourceCustody
  private[floatingpod] def registerRecallSourceContinuation(actualProjection: MainBootProjection,
      lease: MainBootProjection.SourceContinuationLease, server: MinecraftServer,
      originalReply: MySqlProductionBootReply, exactLink: MySqlOwnedIoAdapter.RecallRecoveryLink,
      exactRoot: com.wi.floatingpod.server.source.SourceForestAllocationBudget.Root,
      exactCustody: NormalRecallRecoverySourceCustody, exactProfile: com.wi.floatingpod.config.SourceLimitProfile): Unit =
    verifyClosedBootSaveScopeUse(server)
    require((actualProjection ne null) && borrowedProjection.exists(_ eq actualProjection) &&
      (server eq actualServer) && (originalReply eq reply) && lease != null && exactLink != null &&
      exactRoot != null && exactCustody != null && exactProfile != null,
      "exact original preinit source continuation chain required")
    lease.requireOriginal(actualProjection, this, server, originalReply, exactLink, exactRoot, exactCustody, exactProfile)
    val registerAdapter = recallSourceContinuationLease match
      case Some(existing) =>
        require(existing eq lease, "original source continuation lease is registered once")
        false
      case None =>
        recallSourceContinuationLease = Some(lease)
        true
    if registerAdapter then
      adapter.registerRestrictedSourceContinuation(actualProjection, lease, server, originalReply, exactLink,
        exactRoot, exactCustody, exactProfile, lease.originalEpoch)
  private[floatingpod] def requireRegisteredRecallSourceContinuation(actualProjection: MainBootProjection,
      lease: MainBootProjection.SourceContinuationLease, server: MinecraftServer,
      originalReply: MySqlProductionBootReply, exactLink: MySqlOwnedIoAdapter.RecallRecoveryLink,
      exactRoot: com.wi.floatingpod.server.source.SourceForestAllocationBudget.Root,
      exactCustody: NormalRecallRecoverySourceCustody, exactProfile: com.wi.floatingpod.config.SourceLimitProfile): Unit =
    verifyClosedBootSaveScopeUse(server)
    require(recallSourceContinuationLease.exists(_ eq lease) && borrowedProjection.exists(_ eq actualProjection) &&
      (server eq actualServer) && (reply eq originalReply),
      "original source continuation lease is not registered to this live callbacks route")
    lease.requireOriginal(actualProjection, this, server, originalReply, exactLink, exactRoot, exactCustody, exactProfile)
  /** 同一原 reply 的早期 lease；consume 后仍复用同一对象与 epoch/failure latch。 */
  private[floatingpod] def borrowClaimedProjection(server: MinecraftServer,
      original: MySqlProductionBootReply): com.wi.floatingpod.server.MainBootProjection =
    verifyClosedBootSaveScopeUse(server)
    requireReply(original)
    borrowedProjection match
      case Some(value) => value.requireCurrentReads(); value
      case None =>
        val value = com.wi.floatingpod.server.MainBootProjection.borrowOriginal(this, server, original)
        borrowedProjection = Some(value)
        value
  def ordinarySaveAdmission(): com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission =
    require(ordinarySaveOwner != null, "original ordinary save owner unavailable")
    ordinarySaveOwner
  private var freshSourceBoundary: com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission#FreshSourceBoundary = null
  private val freshCompletionWorlds = new IdentityHashMap[WorldServer, java.lang.Boolean]()
  private var freshDroppedProjection: MainBootProjection = null
  private var freshReadDrop: MainBootProjection.FreshReadDrop = null
  private[floatingpod] def classifyOrdinarySaveSourceProtection(server: MinecraftServer,
      dimension: Int, x: Int, z: Int, chunk: Chunk): MySqlBootSourceProtectionIndex.Classification =
    verifyClosedBootSaveScopeUse(server)
    require(chunk != null && chunk.x == x && chunk.z == z && chunk.getWorld.isInstanceOf[WorldServer],
      "actual ordinary source-save chunk/world required")
    val world = chunk.getWorld.asInstanceOf[WorldServer]
    require((world.getMinecraftServer eq server) && world.provider != null && world.provider.getDimension == dimension,
      "ordinary source-save chunk belongs to another dedicated server/dimension")
    recallSourceContinuationLease match
      case Some(lease) => lease.classifyRetainedSourceProtectionForOrdinaryChunk(dimension, x, z)
      case None => borrowClaimedProjection(server, reply).sourceProtection(dimension, x, z)

  private[floatingpod] def decideManagedSourceProviderChunk(server: MinecraftServer,
      actualProvider: net.minecraft.world.gen.ChunkProviderServer, actualWorld: WorldServer, actualChunk: Chunk,
      operation: com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.ProviderOperation):
      com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.ProviderDecision =
    verifyClosedBootSaveScopeUse(server)
    recallSourceContinuationLease match
      case Some(lease) => lease.decideProviderChunk(actualProvider, actualWorld, actualChunk, operation)
      case None => com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.ProviderDecision.DEFER

  /** 原始 Main 消费已 ACK 的只读 source plan；此处不创建 source selector 或 SQL work。 */
  private[floatingpod] def submitRestrictedSourcePlanDrop(server: MinecraftServer): MySqlRestrictedSourcePlanDropSubmission =
    verifyClosedBootSaveScopeUse(server)
    val lease = recallSourceContinuationLease.getOrElse(
      throw new IllegalStateException("registered original source continuation lease required for plan drop"))
    require(borrowedProjection.nonEmpty && (server eq actualServer),
      "actual claimed Main source projection required for readonly plan drop")
    lease.submitRestrictedSourcePlanDrop()

  /** Main 消费真实 worker ACK 后，Adapter 才释放自己的 plan/receipt 引用。 */
  private[floatingpod] def pollRestrictedSourcePlanDrop(server: MinecraftServer,
      max: Int): MySqlRestrictedSourcePlanDropPoll =
    verifyClosedBootSaveScopeUse(server)
    require(max > 0, "positive bounded source plan drop poll required")
    val lease = recallSourceContinuationLease.getOrElse(
      throw new IllegalStateException("registered original source continuation lease required for plan drop poll"))
    require(borrowedProjection.nonEmpty && (server eq actualServer),
      "actual claimed Main source projection required for readonly plan drop poll")
    lease.pollRestrictedSourcePlanDrop(max) match
      case ready @ MySqlRestrictedSourcePlanDropPoll.Ready(ack) =>
        lease.acknowledgeSourcePlanDrop(adapter, ack)
        try adapter.acknowledgeRestrictedSourcePlanDrop(ack)
        catch
          case cause: Throwable =>
            lease.retainCompletedSourcePlanDropAdapterFailure(adapter, ack, cause)
            throw cause
        ready
      case other => other

  def requireOrdinarySaveCurrent(server: MinecraftServer, dimension: Int, x: Int, z: Int, chunk: Chunk): Unit =
    if ordinarySaveOwner != null && ordinarySaveOwner.isFinalActive() then
      verifyClosedBootSaveScopeUse(server)
      require(chunk != null && chunk.x == x && chunk.z == z && chunk.getWorld.provider.getDimension == dimension,
        "actual final save chunk required")
      ordinarySaveOwner.requireFinalMainSave(chunk.getWorld, x, z, chunk)
    else if freshSourceBoundary != null then
      verifyClosedBootSaveScopeUse(server)
      freshSourceBoundary.requireOriginal(ordinarySaveAdmission())
      require(chunk != null && chunk.x == x && chunk.z == z && chunk.getWorld.isInstanceOf[WorldServer],
        "actual Fresh completion save chunk required")
      val world = chunk.getWorld.asInstanceOf[WorldServer]
      require(freshCompletionWorlds.containsKey(world) && (world.getMinecraftServer eq server) &&
        world.provider.getDimension == dimension, "Fresh completion save belongs to another enrolled world")
      require(!chunk.getEntityLists.exists(_.asScala.exists(_.isInstanceOf[FloatingPodEntity])),
        "Fresh preparation completion cannot save an uncommitted Pod artifact")
    else
      recallSourceContinuationLease match
        case Some(lease) =>
          verifyClosedBootSaveScopeUse(server)
          require(chunk != null && chunk.x == x && chunk.z == z && chunk.getWorld.isInstanceOf[WorldServer],
            "actual restricted source-save chunk/world required")
          val world = chunk.getWorld.asInstanceOf[WorldServer]
          require((world.getMinecraftServer eq server) && world.provider != null &&
            world.provider.getDimension == dimension && world.getChunkProvider.isInstanceOf[net.minecraft.world.gen.ChunkProviderServer],
            "restricted source-save chunk/provider belongs to another server or dimension")
          lease.decideProviderChunk(world.getChunkProvider.asInstanceOf[net.minecraft.world.gen.ChunkProviderServer],
            world, chunk, com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.ProviderOperation.SAVE) match
            case com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.ProviderDecision.ALLOW => ()
            case com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.ProviderDecision.HELD =>
              classifyOrdinarySaveSourceProtection(server, dimension, x, z, chunk) match
                case protectedChunk: MySqlBootSourceProtectionIndex.Protected =>
                  throw new IllegalStateException("Managed chunk has source protection: " + protectedChunk.reasons.mkString(","))
                case unknown: MySqlBootSourceProtectionIndex.Unknown =>
                  val original = unknown.global.flatMap(_.cause).headOption.orNull
                  throw new IllegalStateException("Managed chunk source responsibility is UNKNOWN", original)
                case MySqlBootSourceProtectionIndex.Unprotected =>
                  throw new IllegalStateException(
                    "Managed chunk is held by the exact Fenced source plan because it overlaps an effect or has a global unresolved obligation")
            case com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.ProviderDecision.DEFER =>
              throw new IllegalStateException("Managed source save has no complete outside-unchanged proof")
        case None =>
          classifyOrdinarySaveSourceProtection(server, dimension, x, z, chunk) match
            case MySqlBootSourceProtectionIndex.Unprotected => ()
            case protectedChunk: MySqlBootSourceProtectionIndex.Protected =>
              throw new IllegalStateException("Managed chunk has source protection: " + protectedChunk.reasons.mkString(","))
            case unknown: MySqlBootSourceProtectionIndex.Unknown =>
              val original = unknown.global.flatMap(_.cause).headOption.orNull
              throw new IllegalStateException("Managed chunk source responsibility is UNKNOWN", original)

  /** 真实 Fresh 专用 producer；尚有原保存 token 时返回 None，留给下一真实 END。 */
  private[floatingpod] def beginCertifiedFreshCompletion(server: MinecraftServer,
      runtime: com.wi.floatingpod.network.DedicatedProjectionRuntime,
      projection: MainBootProjection): Option[ClaimedBootstrapExit] =
    verifyClosedBootSaveScopeUse(server)
    runtime.requireFreshPreparationCompletionOwner(this, projection)
    require(claimedExit.isEmpty && freshReadDrop == null && borrowedProjection.exists(_ eq projection) &&
      mainReadProjection.exists(_ eq projection) && reply != null && projection.isCertifiedFreshPreparation,
      "original consumed and borrowed Fresh input required")
    val data = MySqlProductionBootReply.fullTwentyRelationSnapshot(reply).requireBootReadData
    require(data.recoveryRows.isEmpty && data.locators.isEmpty && data.desiredActiveRows.isEmpty,
      "Fresh completion found original Pod/recovery responsibilities")
    require(server.worlds != null && server.worlds.exists(_ != null), "actual Fresh worlds required")
    server.worlds.filter(_ != null).foreach { world =>
      require((world.getMinecraftServer eq server) &&
        !world.loadedEntityList.asScala.exists(_.isInstanceOf[FloatingPodEntity]),
        "Fresh completion cannot abandon a live Pod body")
      world match
        case view: com.wi.floatingpod.mixin.WorldRemovalViewAccessor =>
          require(!view.floatingpod$getUnloadedEntityList().asScala.exists(_.isInstanceOf[FloatingPodEntity]),
            "Fresh completion cannot abandon an unloading Pod body")
        case _ => throw new IllegalStateException("actual world removal view required for Fresh completion")
    }
    val boundary = ordinarySaveAdmission().tryFinishFreshSourceGeneration()
    if boundary == null then None
    else
      freshSourceBoundary = boundary
      server.worlds.filter(_ != null).foreach(world => freshCompletionWorlds.put(world, java.lang.Boolean.TRUE))
      freshDroppedProjection = projection
      freshReadDrop = projection.dropFreshReadInputForCompletion()
      borrowedProjection = None
      mainReadProjection = None
      reply = null
      val exit = new ClaimedBootstrapExit(adapter, claimedMain, approvedFinalizationLimit)
      claimedExit = Some(exit)
      exit.beginCertifiedFresh(this, freshReadDrop)
      Some(exit)

  private[floatingpod] def requireFreshMainInputReleased(actualAdapter: MySqlOwnedIoAdapter,
      drop: MainBootProjection.FreshReadDrop): Unit =
    verifyClosedBootSaveScopeUse(actualServer)
    require((actualAdapter eq adapter) && drop != null && (drop eq freshReadDrop) &&
      freshDroppedProjection != null && borrowedProjection.isEmpty && mainReadProjection.isEmpty && reply == null,
      "actual Fresh Main holder drop required before IO finalization")
    freshSourceBoundary.requireOriginal(ordinarySaveAdmission())
    drop.requireOriginal(freshDroppedProjection, this)
  private var ordinaryMainInputsDropped = false
  private var ordinaryMainEndedReceipt: com.wi.floatingpod.network.DedicatedProjectionRuntime.MainConsumersEnded = null
  private[floatingpod] def dropOrdinaryMainInputs(runtime: com.wi.floatingpod.network.DedicatedProjectionRuntime,
      projection: MainBootProjection): Unit =
    verifyClosedBootSaveScopeUse(actualServer)
    runtime.requireOrdinaryMainDropOwner(this, projection)
    mainReadFailure.foreach(throw _)
    require(!ordinaryMainInputsDropped && approvedMode == ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED &&
      borrowedProjection.exists(_ eq projection) && mainReadProjection.exists(_ eq projection) && reply != null,
      "original Ordinary Main borrowed input required")
    projection.dropOrdinaryOwnedInput(this)
    borrowedProjection = None
    mainReadProjection = None
    reply = null
    projection.requireOrdinaryOwnedInputDropped(this)
    ordinaryMainInputsDropped = true
  private[floatingpod] def requireOrdinaryMainInputsDropped(): Unit =
    verifyClosedBootSaveScopeUse(actualServer)
    mainReadFailure.foreach(throw _)
    ordinarySaveAdmission().requireHealthyAfterFlush()
    require(ordinaryMainInputsDropped && borrowedProjection.isEmpty && mainReadProjection.isEmpty && reply == null,
      "original Main callback fields remain retained")
  /** 原 later END 的应用图边界；不终结 worker/native/root，也不退 final 保存费用。 */
  /** 在任何backing锁外核验真实Runtime、claim、原结果与原policy来源。 */
  private[floatingpod] def requireOrdinaryApplicationRetirementEligibility(
      runtime: com.wi.floatingpod.network.DedicatedProjectionRuntime, actualAdapter: MySqlOwnedIoAdapter,
      actualControl: DedicatedBootstrapBridge, originalPreparation: MySqlOwnedIoAdapter#BootstrapPreparation,
      original: MySqlProductionBootReply.OrdinaryApplicationReply, receipt: Object,
      mode: ProductionBootstrapControl.Mode, limit: Duration): Unit =
    runtime.requireOrdinaryApplicationRetirementOwner(this)
    require((actualAdapter eq adapter) && (actualControl eq control) && (receipt eq originalReceipt) &&
      (preparation eq originalPreparation), "exact original application retirement chain required")
    require((Thread.currentThread() eq claimedMain) && (claimedMain eq actualServer.getServerThread),
      "actual claimed Main required for application retirement")
    requireOrdinaryMainInputsDropped()
    control.requireOrdinaryApplicationPreparation(this, originalReceipt, original)
    require((adapter.requireOrdinaryApplicationResult(control, preparation) eq original) &&
      mode == approvedMode && limit == approvedFinalizationLimit, "original result and policy required")

  private[floatingpod] def tryRetireOrdinaryApplication(runtime: com.wi.floatingpod.network.DedicatedProjectionRuntime): Boolean =
    runtime.requireOrdinaryApplicationRetirementOwner(this)
    require((Thread.currentThread() eq claimedMain) && (claimedMain eq actualServer.getServerThread),
      "actual claimed Main required for application retirement")
    requireOrdinaryMainInputsDropped()
    if !control.ordinaryApplicationControlEnded(this, originalReceipt) then return false
    val originalPreparation = preparation
    val original = adapter.requireOrdinaryApplicationResult(control, originalPreparation)
    control.requireOrdinaryApplicationPreparation(this, originalReceipt, original)
    val mode = approvedMode
    val limit = approvedFinalizationLimit
    adapter.beginOrdinaryApplicationRetirement(control, originalPreparation, original, this, runtime, originalReceipt, mode, limit)
    try
      adapter.retireOrdinaryApplicationPreparation(control, this, originalReceipt, originalPreparation, original)
      control.retireOrdinaryApplicationPreparation(this, originalReceipt, original)
      original match
        case legacy:MySqlOwnedIoReply.OrdinaryCertifiedBooted=>origin=legacy.owned
        case _:MySqlOwnedIoReply.DeploymentReceiptBooted=>
          retiredDeploymentMode=mode;retiredDeploymentLimit=limit;origin=null
      preparationBacking = null
      adapter.requireOrdinaryApplicationPreparationRetired(originalPreparation)
      control.requireOrdinaryApplicationPreparationRetired(this, originalReceipt)
      adapter.finishOrdinaryApplicationRetirement(originalPreparation, original)
      original match
        case legacy:MySqlOwnedIoReply.OrdinaryCertifiedBooted=>legacy.owned.requireRetired()
        case _:MySqlOwnedIoReply.DeploymentReceiptBooted=>
          require(origin==null && retiredDeploymentMode==mode && (retiredDeploymentLimit eq limit),
            "schema5 Main application policy retirement missing")
      true
    catch
      case cause: Throwable =>
        adapter.failOrdinaryApplicationRetirement(originalPreparation, original, cause)
        throw cause

  private[floatingpod] def registerOrdinaryMainEnded(runtime: com.wi.floatingpod.network.DedicatedProjectionRuntime,
      receipt: com.wi.floatingpod.network.DedicatedProjectionRuntime.MainConsumersEnded): Unit =
    requireOrdinaryMainInputsDropped()
    runtime.requireOrdinaryMainEndedReceipt(this, receipt)
    require(ordinaryMainEndedReceipt == null, "original Main-only receipt registration once")
    val identity = adapter.identity
    receipt.consumeOriginal(identity.bootId, identity.bootGeneration, identity.connectionGeneration, receipt.actualEndOrdinal)
    ordinaryMainEndedReceipt = receipt

  private[floatingpod] def requireOrdinaryWorkerHandoff(actualAdapter: MySqlOwnedIoAdapter,
      runtime: com.wi.floatingpod.network.DedicatedProjectionRuntime,
      receipt: com.wi.floatingpod.network.DedicatedProjectionRuntime.MainConsumersEnded): Unit =
    verifyClosedBootSaveScopeUse(actualServer)
    requireOrdinaryMainInputsDropped()
    if (actualAdapter ne adapter) || receipt == null || (ordinaryMainEndedReceipt ne receipt) then
      throw new IllegalStateException("exact registered Ordinary Main receipt required")
    runtime.requireOrdinaryWorkerHandoffOwner(this,receipt)
    receipt.requireRegisteredIdentity(adapter.identity.bootId,adapter.identity.bootGeneration,adapter.identity.connectionGeneration)

  private[floatingpod] def beginCertifiedOrdinaryCompletion(runtime: com.wi.floatingpod.network.DedicatedProjectionRuntime,
      receipt: com.wi.floatingpod.network.DedicatedProjectionRuntime.MainConsumersEnded): ClaimedBootstrapExit =
    requireOrdinaryWorkerHandoff(adapter,runtime,receipt)
    if claimedExit.nonEmpty then throw new IllegalStateException("original claimed exit already attempted")
    val value=new ClaimedBootstrapExit(adapter,claimedMain,approvedFinalizationLimit)
    claimedExit=Some(value) // 原独立O预付；先挂再quiesce/start，失败不重建。
    value.beginCertifiedOrdinary(this,runtime,receipt)
    value

  private[floatingpod] def ordinaryExitObservationPending(runtime: com.wi.floatingpod.network.DedicatedProjectionRuntime,
      receipt: com.wi.floatingpod.network.DedicatedProjectionRuntime.MainConsumersEnded): Boolean =
    requireOrdinaryWorkerHandoff(adapter,runtime,receipt)
    if claimedExit.isEmpty then throw new IllegalStateException("original Ordinary finalizer missing")
    claimedExit.get.ordinaryObservationPending

  private[floatingpod] def requireCertifiedOrdinaryExitCompleted(runtime: com.wi.floatingpod.network.DedicatedProjectionRuntime,
      receipt: com.wi.floatingpod.network.DedicatedProjectionRuntime.MainConsumersEnded): Unit =
    requireOrdinaryWorkerHandoff(adapter,runtime,receipt)
    if claimedExit.isEmpty then throw new IllegalStateException("original Ordinary finalizer missing")
    val original=claimedExit.get
    original.requireOrdinaryObservationReturned()
    val checkpoint=original.checkpoint
    if checkpoint.isEmpty then throw new IllegalStateException("actual Ordinary resource checkpoint missing")
    val observed=checkpoint.get
    if observed.io.fatalCause.nonEmpty then throw observed.io.fatalCause.get
    if observed.io.exitFailure.nonEmpty then throw observed.io.exitFailure.get
    observed.io.workerExitHook match
      case Some(com.wi.floatingpod.server.io.IoWorkerExitHookObservation.HookReturned) => ()
      case _ => throw new IllegalStateException("actual complete Ordinary exit hook did not return")
    if !observed.io.stopped || observed.retainedAllocation.nonEmpty then throw new IllegalStateException("actual Ordinary worker/static graph remains owned")
    observed.resources match
      case Some(MySqlOwnedIoResourceObservation.DeploymentReceiptObserved(failure,retained)) =>
        failure.foreach(throw _)
        if origin!=null || retiredDeploymentMode!=ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED || retained then
          throw new IllegalStateException("actual schema5 Ordinary application/native/static/root completion is incomplete")
      case Some(MySqlOwnedIoResourceObservation.SessionObserved(closed)) =>
        if closed.failure.nonEmpty then throw closed.failure.get
        val inspection=closed.certifiedInspection
        if closed.bootId != adapter.identity.bootId || closed.bootGeneration != adapter.identity.bootGeneration ||
            closed.connectionGeneration != adapter.identity.connectionGeneration || closed.release != MySqlOwnedSessionRelease.Released ||
            !closed.releaseReturned || !closed.jdbcClosed.contains(true) || closed.unresolvedOwnedResponsibility ||
            (inspection.isEmpty || inspection.get.status != "ORDINARY_NATIVE_AND_ROOT_CLOSED" ||
              inspection.get.retainedNativeResponsibility || inspection.get.originalRootRetained || inspection.get.failure.nonEmpty) then
          throw new IllegalStateException("actual Ordinary writer/JDBC/native/borrow/root completion is incomplete")
      case _ => throw new IllegalStateException("actual Ordinary Session close observation required")

  private[floatingpod] def detachOrdinaryExitObservation(runtime: com.wi.floatingpod.network.DedicatedProjectionRuntime): Unit =
    runtime.requireOrdinaryFinalSaveEndOwner(this)
    requireCertifiedOrdinaryExitCompleted(runtime,ordinaryMainEndedReceipt)
    claimedExit=None
    ordinaryMainEndedReceipt=null

  /** Every managed constructor/save boundary rechecks the exact retained Main/context; no open state exists. */
  def verifyClosedBootSaveScopeUse(server: MinecraftServer): Unit =
    require((server eq actualServer) && claimedMain != null &&
      (Thread.currentThread() eq claimedMain) && (claimedMain eq actualServer.getServerThread),
      "exact claimed server Main required for managed save classification")
    worldContext.validateAboutToStart(server)
  override def abortTransfer(value: Object, receipt: Object, originalCause: Throwable): Unit =
    requireReply(value); require(originalCause != null, "original failure required")
    adapter.abortOwnerTransfer(requireReceipt(receipt), originalCause) // actual adapter enforces owner/NEW/TERMINATED target.
  override def finalizeFailure(value: Object, receipt: Object, originalCause: Throwable): CompletionStage[Void] =
    owner(); require(originalCause != null && claimedMain == null, "old owner must not finalize claimed resources")
    // Bridge refuses unresolved pending receipt; holder then settles only actual reservation/exit observations.
    try finalPreparation = Some(preparation.finalizePreparation(approvedFinalizationLimit))
    catch case cleanup: Throwable =>
      finalPreparation = Some(preparation.observation)
      if cleanup ne originalCause then originalCause.addSuppressed(cleanup)
    try adapter.quiesce()
    catch case cleanup: Throwable => if cleanup ne originalCause then originalCause.addSuppressed(cleanup)
    ProductionDedicatedBootstrapCallbacks.finalizeOffOwner(adapter, approvedFinalizationLimit, originalCause,
      checkpoint => finalCheckpoint = checkpoint)
  override def failedStartupTermination(report: DedicatedBootstrapBridge.FailureReport): Unit =
    owner(); require(report != null && (report.actualServer() eq actualServer), "actual dedicated failure report required")
    failureReport = report // original cause/receipt/disposition remain strongly reachable even when finalization failed.
    ProductionDedicatedBootstrapCallbacks.safeFailure(report.originalCause(), new IdentityHashMap(), "startup")
    ProductionBootstrapInstallation.invokeStartupFailureShutdown(actualServer, this, report)
  def requireOriginalStartupFailure(report: DedicatedBootstrapBridge.FailureReport): Unit =
    owner()
    require(report != null && (report eq failureReport) && (report.actualServer() eq actualServer) && claimedMain == null,
      "original preclaim startup failure report required")
  /** only complete AboutToStart consumer: no IO wait/config reread; this is not GameReady. */
  def consumeClaimedPreparation(server: MinecraftServer): MySqlProductionBootReply =
    require(server eq actualServer, "actual dedicated instance required")
    require(claimedMain != null && (Thread.currentThread() eq claimedMain) && control.phase() == DedicatedBootstrapBridge.Phase.CLAIMED,
      "exact successfully claimed Main required")
    require(!claimedPreparationConsumed, "claimed preparation is consumed once")
    claimedPreparationConsumed = true // 验证失败也保留消费责任，禁止以同结果重新装配。
    worldContext.validateAboutToStart(server)
    require(reply != null, "actual preparation reply unavailable")
    reply
  private var mainReadProjection = Option.empty[com.wi.floatingpod.server.MainBootProjection]
  private var mainReadFailure = Option.empty[Throwable]
  /** 原 holder 保留消费失败与已发出的读 lease；不等待 IO，也不签发 Ready。 */
  private[floatingpod] def consumeClaimedMainProjection(server: MinecraftServer): com.wi.floatingpod.server.MainBootProjection =
    require((server eq actualServer) && (Thread.currentThread() eq claimedMain), "exact claimed instance/Main required")
    try
      val result = com.wi.floatingpod.server.MainBootProjection.consumeOriginal(this, server)
      mainReadProjection = Some(result)
      result
    catch
      case cause: Throwable =>
        if mainReadFailure.isEmpty then mainReadFailure = Some(cause)
        throw cause
  private[floatingpod] def retainedMainReadFailure: Option[Throwable] = mainReadFailure
  private var claimedExit = Option.empty[ClaimedBootstrapExit]
  /** Main 只发 quiesce；原 holder 保留真实 off-Main join/close 观察，绝不调用 preclaim finalizer。 */
  private[floatingpod] def stopClaimed(server: MinecraftServer, original: Throwable): ClaimedBootstrapExit =
    require((server eq actualServer) && (Thread.currentThread() eq claimedMain), "exact claimed Main stop required")
    require(original != null, "original retained stop cause required")
    if ordinarySaveOwner != null then ordinarySaveOwner.stop(original)
    claimedExit match
      case Some(value) => value.retainFailure(original); value
      case None =>
        val value = new ClaimedBootstrapExit(adapter, claimedMain, approvedFinalizationLimit)
        claimedExit = Some(value) // before quiesce/thread start: every failure remains in original holder.
        value.begin(original)
        value
  private[floatingpod] def observedClaimedExit: Option[ClaimedBootstrapExit] = claimedExit
  /** 只能读取原 finalizer 的实际记录；不接受外部 close DATA 或 caller 成功布尔。 */
  def requireCertifiedFreshExitCompleted(server: MinecraftServer): Unit =
    verifyClosedBootSaveScopeUse(server)
    require(freshReadDrop != null && freshDroppedProjection != null && reply == null,
      "original Fresh Main source release required")
    freshSourceBoundary.requireOriginal(ordinarySaveAdmission())
    val original = claimedExit.getOrElse(throw new IllegalStateException("original Fresh finalizer missing"))
    original.requireFreshObservationReturned()
    original.failure.foreach(throw _)
    original.waitObservation match
      case Some(MySqlOwnedIoWorkerTerminationWait.Terminated(proof)) => adapter.requireOwnWorkerTerminationProof(proof)
      case _ => throw new IllegalStateException("actual Fresh worker termination not observed")
    val observed = original.checkpoint.getOrElse(throw new IllegalStateException("actual Fresh resource checkpoint missing"))
    observed.io.fatalCause.foreach(throw _)
    observed.io.exitFailure.foreach(throw _)
    require(observed.io.stopped && observed.retainedAllocation.isEmpty &&
      observed.io.workerExitHook.contains(com.wi.floatingpod.server.io.IoWorkerExitHookObservation.HookReturned),
      "actual Fresh IO worker/static allocation or hook remains unsettled")
    observed.resources match
      case Some(MySqlOwnedIoResourceObservation.DeploymentReceiptObserved(failure,retained))=>
        failure.foreach(throw _)
        require(approvedMode==ProductionBootstrapControl.Mode.FRESH && !retained,
          "actual schema5 Fresh native/static/root completion is incomplete")
      case Some(MySqlOwnedIoResourceObservation.SessionObserved(closed)) =>
        closed.failure.foreach(throw _)
        require(closed.bootId == adapter.identity.bootId && closed.bootGeneration == adapter.identity.bootGeneration &&
          closed.connectionGeneration == adapter.identity.connectionGeneration && closed.release == MySqlOwnedSessionRelease.Released &&
          closed.releaseReturned && closed.jdbcClosed.contains(true) && !closed.unresolvedOwnedResponsibility &&
          closed.freshResponsibility.exists(value => value.certifiedWitnessesReleased &&
            value.native.retention == MySqlFreshNativeRetention.CertifiedCompletionClosed),
          "original Fresh writer/JDBC/native/barrier/root completion is incomplete")
      case _ => throw new IllegalStateException("actual Fresh session close observation required")
    ordinarySaveAdmission().requireHealthyAfterFlush()
  def observedFailure: Option[DedicatedBootstrapBridge.FailureReport] = Option(failureReport)
  def observedExitCheckpoint: Option[MySqlOwnedIoCheckpoint] = Option(finalCheckpoint)
  def observedFinalPreparation: Option[IoPreparationObservation] = finalPreparation

object ProductionDedicatedBootstrapCallbacks:
  // root前的有限输入holder：只保已核typed控制/metadata/profile；不持Loaded B/K/C或SQL/native资源。
  private enum Origin:
    case Legacy(value: ProductionBootstrapControl)
    case V3(value: ProductionBootstrapControl.StaticV3)
    case DeploymentReceipt(value: ProductionBootstrapControl.DeploymentReceiptControl)
  private[floatingpod] def freshPlan(metadata: MySqlRuntimeMetadata,
      fresh: ProductionBootstrapControl.FreshIdentity): WindowsWorldProvisioner.Plan =
    require(metadata != null && fresh != null, "approved Format3 metadata and actual Fresh identity required")
    val baseline = metadata.baseline()
    require(baseline.schemaFormat() == MySqlApprovedBaselineLoader.Format3SchemaFormat &&
      baseline.coverage() == MySqlAuditCoverage.Format3,
      "production Fresh plan requires exact Format3 schema metadata")
    new WindowsWorldProvisioner.Plan(fresh.creation(), fresh.bootstrap(), fresh.prepare(), fresh.certify(),
      metadata.expectedBindingUuid(), 2, metadata.expectedServerUuid(), metadata.schema(),
      baseline.schemaFormat(), baseline.ddlSha256(), baseline.provisioningUuid())

  def open(actual: Object, control: DedicatedBootstrapBridge, input: ProductionBootstrapControl,
           dependencies: EarlyDependencyResolver.VerifiedDependencies): DedicatedBootstrapBridge.Callbacks =
    require(control != null && input != null && dependencies != null && (Thread.currentThread() eq control.controlThread()) &&
      control.phase() == DedicatedBootstrapBridge.Phase.PREPARING, "exact actual bootstrap control required")
    val server = actual match
      case value: MinecraftServer if value.isDedicatedServer => value
      case _ => throw new IllegalArgumentException("Actual dedicated MinecraftServer required")
    require(dependencies.businessBackend() == "MYSQL", "verified MYSQL dependency assembly required")
    require(input.isProductionFormatV2(), "production factory requires V2 mode-specific control")
    val world = DedicatedWorldContext.capture(server)
    val metadata = input.metadata()
    require(WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(metadata).configurationSha256().equalsIgnoreCase(input.configurationSha256()),
      "approved canonical configuration differs before adapter/native/socket")
    val policy = MySqlRuntimeAuditPolicy(input.policy().callerRoles(),input.policy().auditDefinerRoles(),MySqlExpectedNoImport,
      MySqlExpectedPhysicalFormat.Format2,input.policy().receiptFreshness())
    val controlled = MySqlFreshControlledMetadata(input.metadataPath(),input.configurationSha256())
    val command = input.mode() match
      case ProductionBootstrapControl.Mode.FRESH =>
        input.requireFreshWorld(world.worldDirectory())
        input.consumeFreshIdentityOnce() // sticky original identities; no failure retry/remint.
        val fresh = input.fresh()
        val boot = new WindowsWorldProvisioner.BootIdentity(fresh.boot(),fresh.bootGeneration())
        MySqlOwnedIoCommand.BootFresh(metadata,policy,input.policy().writerLease(),
          MySqlFreshBootRequest(world.worldDirectory(),freshPlan(metadata,fresh),boot,controlled))
      case ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED =>
        input.requireOrdinaryWorld(world.worldDirectory())
        input.consumeOrdinaryIdentityOnce() // burns only this current generation; birth IDs stay source DATA.
        val ordinary = input.ordinary()
        val boot = new WindowsWorldProvisioner.BootIdentity(ordinary.boot(),ordinary.bootGeneration())
        MySqlOwnedIoCommand.BootOrdinaryCertified(metadata,policy,input.policy().writerLease(),
          MySqlOrdinaryCertifiedBootRequest(world.worldDirectory(),input.originalCreation(),boot,ordinary,controlled,
            input.runtimeContractSha256()))
    val boot = command match
      case MySqlOwnedIoCommand.BootFresh(_,_,_,request) => request.boot
      case MySqlOwnedIoCommand.BootOrdinaryCertified(_,_,_,request) => request.boot
      case _ => throw new IllegalStateException("Mode-specific production command was not constructed")
    val currentIdentity = command match
      case MySqlOwnedIoCommand.BootFresh(_,_,_,request) =>
        val value = input.fresh(); MySqlOwnedIoIdentity(value.boot(),value.bootGeneration(),value.connectionGeneration())
      case MySqlOwnedIoCommand.BootOrdinaryCertified(_,_,_,request) =>
        val value = input.ordinary(); MySqlOwnedIoIdentity(value.boot(),value.bootGeneration(),value.connectionGeneration())
      case _ => throw new IllegalStateException("Mode-specific production command was not constructed")
    require(currentIdentity.bootId == boot.bootId() && currentIdentity.bootGeneration == boot.generation(),
      "production command boot must be the exact pinned current generation")
    val submitOperation = command match
      case MySqlOwnedIoCommand.BootFresh(_,_,_,_) => input.fresh().submitOperation()
      case MySqlOwnedIoCommand.BootOrdinaryCertified(_,_,_,_) => input.ordinary().submitOperation()
      case _ => throw new IllegalStateException("Mode-specific production command was not constructed")
    val identity = currentIdentity
    val tag = IoOperationTag(identity.bootId,identity.bootGeneration,identity.connectionGeneration,submitOperation)
    val adapter = new MySqlOwnedIoAdapter(1,Thread.currentThread(),"FloatingPod production owned IO",identity,enableAdmissionMaintenance=true)
    try
      val holder = command match
        case fresh: MySqlOwnedIoCommand.BootFresh => adapter.installFreshBootstrapPreparation(control,tag,fresh)
        case ordinary: MySqlOwnedIoCommand.BootOrdinaryCertified => adapter.installOrdinaryCertifiedBootstrapPreparation(control,tag,ordinary)
        case _ => throw new IllegalStateException("Mode-specific production command was not constructed")
      val callbacks = new ProductionDedicatedBootstrapCallbacks(server,control,Origin.Legacy(input),world,dependencies,adapter,tag,holder)
      ProductionBootstrapInstallation.retain(server,control,callbacks)
      callbacks
    catch
      case original: Throwable =>
        try adapter.quiesce()
        catch case cleanup: Throwable => if cleanup ne original then original.addSuppressed(cleanup)
        // No command has been submitted yet. Still observe the real worker's bounded exit off-owner.
        try finalizeOffOwner(adapter,input.policy().limits().finalization(),original,_ => ()).toCompletableFuture.get(
          input.policy().limits().finalization().toNanos,TimeUnit.NANOSECONDS)
        catch case cleanup: Throwable =>
          val cause = cleanup match
            case e: java.util.concurrent.ExecutionException if e.getCause != null => e.getCause
            case other => other
          if cause ne original then original.addSuppressed(cause)
        throw original
  private[floatingpod] def commandV3(input: ProductionBootstrapControl.StaticV3, actualWorldPath: java.nio.file.Path): MySqlOwnedIoCommand =
    require(input != null && actualWorldPath != null,"actual typed StaticV3 and explicit world path required")
    val controlled=MySqlFreshControlledMetadata(input.metadataPath(),input.configurationSha256())
    input.mode() match
      case ProductionBootstrapControl.Mode.FRESH =>
        input.requireFreshWorld(actualWorldPath)
        val fresh=input.fresh();val metadata=input.metadata();val baseline=metadata.baseline()
        val plan=new WindowsWorldProvisioner.Plan(fresh.creation(),fresh.bootstrap(),fresh.prepare(),fresh.certify(),
          metadata.expectedBindingUuid(),2,metadata.expectedServerUuid(),metadata.schema(),baseline.schemaFormat(),baseline.ddlSha256(),baseline.provisioningUuid())
        val boot=new WindowsWorldProvisioner.BootIdentity(fresh.boot(),fresh.bootGeneration())
        MySqlOwnedIoCommand.BootFreshV3(input,MySqlFreshBootRequest(actualWorldPath,plan,boot,controlled))
      case ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED =>
        input.requireOrdinaryWorld(actualWorldPath)
        val ordinary=input.ordinary();val boot=new WindowsWorldProvisioner.BootIdentity(ordinary.boot(),ordinary.bootGeneration())
        MySqlOwnedIoCommand.BootOrdinaryV3(input,MySqlOrdinaryCertifiedBootRequest(actualWorldPath,input.originalCreation(),boot,
          ordinary,controlled,input.runtimeContractSha256()))

  /** V3始终保真实typed输入；worker唯一消费once，原legacy open路径不变。 */
  def openV3(actual: Object, control: DedicatedBootstrapBridge, input: ProductionBootstrapControl.StaticV3,
      dependencies: EarlyDependencyResolver.VerifiedDependencies): DedicatedBootstrapBridge.Callbacks =
    require(control != null && input != null && dependencies != null && (Thread.currentThread() eq control.controlThread()) &&
      control.phase()==DedicatedBootstrapBridge.Phase.PREPARING,"exact actual bootstrap control required")
    val server=actual match
      case value: MinecraftServer if value.isDedicatedServer => value
      case _ => throw new IllegalArgumentException("Actual dedicated MinecraftServer required")
    require(dependencies.businessBackend()=="MYSQL","verified MYSQL dependency assembly required")
    val world=DedicatedWorldContext.capture(server)
    val command=commandV3(input,world.worldDirectory())
    val identity=input.mode() match
      case ProductionBootstrapControl.Mode.FRESH =>
        val value=input.fresh();MySqlOwnedIoIdentity(value.boot(),value.bootGeneration(),value.connectionGeneration())
      case ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED =>
        val value=input.ordinary();MySqlOwnedIoIdentity(value.boot(),value.bootGeneration(),value.connectionGeneration())
    val operation=input.mode() match
      case ProductionBootstrapControl.Mode.FRESH => input.fresh().submitOperation()
      case ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED => input.ordinary().submitOperation()
    val tag=IoOperationTag(identity.bootId,identity.bootGeneration,identity.connectionGeneration,operation)
    val adapter=new MySqlOwnedIoAdapter(1,Thread.currentThread(),"FloatingPod production owned IO",identity,enableAdmissionMaintenance=true)
    try
      val holder=command match
        case fresh: MySqlOwnedIoCommand.BootFreshV3 => adapter.installFreshBootstrapPreparationV3(control,tag,fresh)
        case ordinary: MySqlOwnedIoCommand.BootOrdinaryV3 => adapter.installOrdinaryCertifiedBootstrapPreparationV3(control,tag,ordinary)
        case _ => throw new IllegalStateException("Exact V3 production command was not constructed")
      val callbacks=new ProductionDedicatedBootstrapCallbacks(server,control,Origin.V3(input),world,dependencies,adapter,tag,holder)
      ProductionBootstrapInstallation.retain(server,control,callbacks)
      callbacks
    catch
      case original: Throwable =>
        try adapter.quiesce()
        catch case cleanup: Throwable => if cleanup ne original then original.addSuppressed(cleanup)
        try finalizeOffOwner(adapter,input.policy().limits().finalization(),original,_ => ()).toCompletableFuture.get(
          input.policy().limits().finalization().toNanos,TimeUnit.NANOSECONDS)
        catch
          case cleanup: Throwable =>
            val cause=cleanup match
              case e: java.util.concurrent.ExecutionException if e.getCause != null => e.getCause
              case other => other
            if cause ne original then original.addSuppressed(cause)
        throw original

  /** schema5 DATA 请求保持独立；once 身份只在原 worker 验证 envelope 后消费。 */
  private[floatingpod] def commandDeploymentReceipt(input: ProductionBootstrapControl.DeploymentReceiptControl,
      actualWorldPath: java.nio.file.Path): MySqlOwnedIoCommand =
    require(input != null && actualWorldPath != null, "actual schema5 control/world required")
    val controlled = MySqlFreshControlledMetadata(input.metadataPath(), input.configurationSha256())
    input.mode() match
      case ProductionBootstrapControl.Mode.FRESH =>
        input.requireFreshWorld(actualWorldPath)
        val f = input.fresh()
        val m = input.metadata().deploymentSemantics().canonicalFields()
        val b = input.metadata().baseline()
        val plan = new com.wi.floatingpod.server.WindowsWorldProvisionReceiptDataCodec.PlanData(
          f.creation(), f.bootstrap(), f.prepare(), f.certify(), java.util.UUID.fromString(m.get("expectedBindingUuid")),
          2, java.util.UUID.fromString(m.get("expectedServerUuid")), m.get("schema"), 5, b.ddlSha256(), b.provisioningUuid())
        MySqlOwnedIoCommand.BootFresh5(input, actualWorldPath, plan,
          new WindowsWorldProvisioner.BootIdentity(f.boot(), f.bootGeneration()), controlled)
      case ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED =>
        input.requireOrdinaryWorld(actualWorldPath)
        MySqlOwnedIoCommand.BootOrdinary5(input, actualWorldPath, input.ordinary(), input.originalSourcePins(), controlled)

  def openDeploymentReceipt(actual: Object, control: DedicatedBootstrapBridge,
      input: ProductionBootstrapControl.DeploymentReceiptControl,
      dependencies: EarlyDependencyResolver.VerifiedDependencies): DedicatedBootstrapBridge.Callbacks =
    require(control != null && input != null && dependencies != null &&
      (Thread.currentThread() eq control.controlThread()) && control.phase() == DedicatedBootstrapBridge.Phase.PREPARING,
      "exact actual bootstrap control required")
    val server = actual match
      case value: MinecraftServer if value.isDedicatedServer => value
      case _ => throw new IllegalArgumentException("Actual dedicated MinecraftServer required")
    require(dependencies.businessBackend() == "MYSQL", "verified MYSQL dependency assembly required")
    val world = DedicatedWorldContext.capture(server)
    val command = commandDeploymentReceipt(input, world.worldDirectory())
    val (identity, operation) = input.mode() match
      case ProductionBootstrapControl.Mode.FRESH =>
        val f = input.fresh()
        (MySqlOwnedIoIdentity(f.boot(), f.bootGeneration(), f.connectionGeneration()), f.submitOperation())
      case ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED =>
        val o = input.ordinary()
        (MySqlOwnedIoIdentity(o.boot(), o.bootGeneration(), o.connectionGeneration()), o.submitOperation())
    val tag = IoOperationTag(identity.bootId, identity.bootGeneration, identity.connectionGeneration, operation)
    val adapter = new MySqlOwnedIoAdapter(1, Thread.currentThread(), "FloatingPod production owned IO", identity,
      enableAdmissionMaintenance = true)
    try
      val holder = command match
        case fresh: MySqlOwnedIoCommand.BootFresh5 => adapter.installFreshBootstrapPreparation5(control, tag, fresh)
        case ordinary: MySqlOwnedIoCommand.BootOrdinary5 => adapter.installOrdinaryBootstrapPreparation5(control, tag, ordinary)
        case _ => throw new IllegalStateException("Exact schema5 production command was not constructed")
      val callbacks = new ProductionDedicatedBootstrapCallbacks(server, control, Origin.DeploymentReceipt(input), world,
        dependencies, adapter, tag, holder)
      ProductionBootstrapInstallation.retain(server, control, callbacks)
      callbacks
    catch
      case original: Throwable =>
        try adapter.quiesce()
        catch case cleanup: Throwable => if cleanup ne original then original.addSuppressed(cleanup)
        try finalizeOffOwner(adapter, input.policy().limits().finalization(), original, _ => ()).toCompletableFuture.get(
          input.policy().limits().finalization().toNanos, TimeUnit.NANOSECONDS)
        catch
          case cleanup: Throwable =>
            val cause = cleanup match
              case e: java.util.concurrent.ExecutionException if e.getCause != null => e.getCause
              case other => other
            if cause ne original then original.addSuppressed(cause)
        throw original

  private[bootstrap] def finalizeOffOwner(adapter: MySqlOwnedIoAdapter, limit: Duration, original: Throwable,
                               retain: MySqlOwnedIoCheckpoint => Unit): CompletionStage[Void] =
    val result = new CompletableFuture[Void]()
    val body: Runnable = () => {
      try
        val exited = adapter.awaitExitOffMain(limit.toNanos,TimeUnit.NANOSECONDS)
        val checkpoint = adapter.checkpoint
        retain(checkpoint)
        checkpoint.io.fatalCause.foreach(cause => if cause ne original then original.addSuppressed(cause))
        checkpoint.io.exitFailure.foreach(cause => if cause ne original then original.addSuppressed(cause))
        require(exited && checkpoint.io.stopped && !checkpoint.io.running, "actual owned worker exit remains unobserved")
        result.complete(null)
      catch case cause: Throwable => result.completeExceptionally(cause)
    }
    val tail = new Thread(body,"FloatingPod bootstrap failure finalizer")
    try tail.start()
    catch case cause: Throwable => result.completeExceptionally(cause)
    result.minimalCompletionStage()
  private[bootstrap] def safeFailure(cause: Throwable, seen: IdentityHashMap[Throwable, java.lang.Boolean], edge: String): Unit =
    if cause != null && !seen.containsKey(cause) then
      seen.put(cause,true)
      System.err.println(s"FloatingPod bootstrap failure edge=$edge class=${cause.getClass.getName}")
      cause.getStackTrace.foreach(frame => System.err.println("FRAME " + frame))
      cause match
        case mismatch: com.wi.floatingpod.storage.mysql.MySqlSchemaAuditor.StaticManifestMismatch =>
          System.err.println("STATIC_MANIFEST_DIFFERENCE " + mismatch.safeDifference)
        case mismatch: com.wi.floatingpod.storage.mysql.MySqlDbFirstMetadataMismatch =>
          System.err.println("FULL27_JDBC_DIFFERENCE " + mismatch.safeDifference)
        case inspection: com.wi.floatingpod.storage.mysql.MySqlDbFirstAbandonedStartupInspection =>
          System.err.println("ABANDONED_STARTUP_INSPECTION " + inspection.safeSummary)
        case _ => ()
      safeFailure(cause.getCause,seen,"cause")
      cause.getSuppressed.foreach(value => safeFailure(value,seen,"suppressed"))
      cause match
        case failure: SqlJdbcException =>
          val diagnostic = failure.diagnostic()
          System.err.println(s"DIAGNOSTIC id=${diagnostic.diagnosticId()} stage=${diagnostic.stage()} backend=${diagnostic.backend()} code=${diagnostic.code()} truncated=${diagnostic.truncated()}")
          diagnostic.nodes().asScala.foreach(n => System.err.println(s"NODE id=${n.id()} stage=${n.stage()} category=${n.category()} state=${n.sqlState()} vendor=${n.vendorCode()}"))
          diagnostic.edges().asScala.foreach(e => System.err.println(s"EDGE from=${e.from()} to=${e.to()} relation=${e.relation()}"))
        case sql: java.sql.SQLException =>
          System.err.println(s"SQL state=${sql.getSQLState} vendor=${sql.getErrorCode}")
          safeFailure(sql.getNextException,seen,"nextException")
        case _ => ()
