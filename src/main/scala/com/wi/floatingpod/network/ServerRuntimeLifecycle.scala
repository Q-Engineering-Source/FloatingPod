package com.wi.floatingpod.network

import com.wi.floatingpod.bootstrap.{ProductionBootstrapInstallation,ProductionDbFirstBootstrapCallbacks,ProductionDedicatedBootstrapCallbacks}
import com.wi.floatingpod.server.{ActiveFloatingPodManager, CertifiedWorldBindingGate, ChunkLoadingManager, DeferredRecoveryBundle, LiveCrashRecovery, OwnershipValidator, PodWorldImpl, PositionValidator, RecoveryExit, RecoveryFinishReport, RecoveryStep, RuntimeRecoveryTermination, ServerRecoveryTickPhase, ServerStartupRecovery, ServerWorldResolver, StartupPublicationDecision, StartupRecoveryReport, WorldBindingBootstrap}
import com.wi.floatingpod.server.persistence.{PeriodicFlushService, PersistenceManager, PersistenceStoppingReport}
import com.wi.floatingpod.server.save.ManagedSourceSaveLedger
import com.wi.floatingpod.storage.{ActivityTransitionStore, FloatingPodRepository, PhysicalRecoveryStore, PlayerEntitlementStore, RecoveryPodStore}
import com.wi.floatingpod.storage.sqlite.SQLiteFloatingPodRepository

import net.minecraftforge.common.config.Configuration
import net.minecraft.server.MinecraftServer
import net.minecraftforge.fml.common.event.{FMLServerAboutToStartEvent, FMLServerStartingEvent, FMLServerStoppedEvent, FMLServerStoppingEvent}
import org.apache.logging.log4j.{Level, LogManager}

import java.io.File
import java.nio.file.{Files, Path}
import java.util.Locale
import scala.util.{Failure, Success}

/** 服务端私有运行设置；当前只允许已经实现的 SQLite 后端。 */
final case class ServerRuntimeSettings private (backend: String, sqlitePath: Path, activeLimit: Int)

/** 仅暴露服务端设置加载所需的 Forge Configuration 行为，避免 runtime 细节泄漏到校验逻辑。 */
trait ServerRuntimeConfiguration:
  def load(): Unit
  def string(key: String, category: String, defaultValue: String, comment: String): String
  def hasChanged: Boolean
  def save(): Unit

/** 将 Forge Configuration 收敛为服务端私有设置读取边界。 */
final class ForgeServerRuntimeConfiguration(configuration: Configuration) extends ServerRuntimeConfiguration:
  require(configuration != null, "configuration must not be null")
  override def load(): Unit = configuration.load()
  override def string(key: String, category: String, defaultValue: String, comment: String): String =
    configuration.getString(key, category, defaultValue, comment)
  override def hasChanged: Boolean = configuration.hasChanged
  override def save(): Unit = configuration.save()

object ServerRuntimeSettings:
  private val Category = "server"
  private val BackendKey = "backend"
  private val SqlitePathKey = "sqlitePath"
  private val ActiveLimitKey = "activeLimit"
  private val SqliteBackend = "sqlite"
  private val DefaultActiveLimit = 256

  /** 从 FML pre-init 建议的配置文件加载并校验不可变服务端设置。 */
  def load(suggestedConfigurationFile: File): ServerRuntimeSettings =
    require(suggestedConfigurationFile != null, "suggested configuration file must not be null")
    load(new ForgeServerRuntimeConfiguration(new Configuration(suggestedConfigurationFile)), suggestedConfigurationFile)

  /** 从已创建的 Forge 设置边界加载；默认值只在底层配置确有变更时保存。 */
  def load(configuration: ServerRuntimeConfiguration, suggestedConfigurationFile: File): ServerRuntimeSettings =
    require(configuration != null, "configuration must not be null")
    require(suggestedConfigurationFile != null, "suggested configuration file must not be null")
    configuration.load()
    try
      val configFile = suggestedConfigurationFile.toPath.toAbsolutePath.normalize
      val configDirectory = Option(configFile.getParent).getOrElse(
        throw new IllegalArgumentException("suggested configuration file must have a parent directory")
      )
      val defaultPath = configDirectory.resolve("floatingpod").resolve("floatingpod.db").toAbsolutePath.normalize
      val backend = configuration.string(BackendKey, Category, SqliteBackend, "当前仅支持 sqlite 后端")
      val sqlitePath = configuration.string(SqlitePathKey, Category, defaultPath.toString, "SQLite 数据库文件路径，仅供服务端使用")
      val activeLimit = configuration.string(ActiveLimitKey, Category, DefaultActiveLimit.toString, "同时活动的 FloatingPod 上限（1..256）")
      fromValues(backend, sqlitePath, activeLimit, configDirectory)
    finally
      if configuration.hasChanged then configuration.save()

  /** 为已验证的启动设置提供直接构造入口，不读取或保存任何配置文件。 */
  def sqlite(sqlitePath: Path, activeLimit: Int): ServerRuntimeSettings =
    require(sqlitePath != null, "sqlitePath must not be null")
    build(SqliteBackend, sqlitePath.toAbsolutePath.normalize, activeLimit)

  private def fromValues(backendValue: String, pathValue: String, limitValue: String, configDirectory: Path): ServerRuntimeSettings =
    val backend = Option(backendValue).map(_.trim.toLowerCase(Locale.ROOT)).getOrElse("")
    if backend != SqliteBackend then
      throw new IllegalArgumentException(s"Unsupported FloatingPod storage backend: $backendValue")

    val rawPath = Option(pathValue).map(_.trim).getOrElse("")
    if rawPath.isEmpty then throw new IllegalArgumentException("SQLite path must not be blank")
    val configuredPath = Path.of(rawPath)
    val absolutePath =
      if configuredPath.isAbsolute then configuredPath.toAbsolutePath.normalize
      else configDirectory.resolve(configuredPath).toAbsolutePath.normalize

    val activeLimit =
      try limitValue.trim.toInt
      catch
        case cause: NumberFormatException => throw new IllegalArgumentException("FloatingPod active limit must be an integer", cause)
    build(backend, absolutePath, activeLimit)

  private def build(backend: String, sqlitePath: Path, activeLimit: Int): ServerRuntimeSettings =
    require(backend == SqliteBackend, "backend must be sqlite")
    require(sqlitePath != null, "sqlitePath must not be null")
    require(activeLimit >= 1 && activeLimit <= DefaultActiveLimit, "activeLimit must be between 1 and 256")
    ServerRuntimeSettings(SqliteBackend, sqlitePath.toAbsolutePath.normalize, activeLimit)

/** 将 PersistenceManager 生命周期收敛为 runtime 可替换的窄边界。 */
trait RuntimePersistence:
  def start(event: FMLServerStartingEvent): Unit
  def stop(event: FMLServerStoppingEvent): Unit
  /** 撤销已经成功启动但尚未进入正常停服路径的后台资源。 */
  def abortStarted(): Unit
  def stopWithReport(event: FMLServerStoppingEvent): Option[PersistenceStoppingReport] =
    stop(event)
    None

/** 将已实现的 PersistenceManager 适配为运行时生命周期边界，并保留原始失败。 */
final class PersistenceManagerRuntime(persistence: PersistenceManager) extends RuntimePersistence:
  require(persistence != null, "persistence must not be null")

  override def start(event: FMLServerStartingEvent): Unit =
    persistence.onServerStarting(event) match
      case Success(_) => ()
      case Failure(problem) => throw problem

  override def stop(event: FMLServerStoppingEvent): Unit =
    persistence.onServerStopping(event) match
      case Success(_) => ()
      case Failure(problem) => throw problem

  override def stopWithReport(event: FMLServerStoppingEvent): Option[PersistenceStoppingReport] = Some(persistence.stopWithReport(event))
  override def abortStarted(): Unit = persistence.abortStarted()

/** 隔离全局 packet endpoint 安装状态，供运行时使用和直接测试替换。 */
trait RuntimeEndpointRegistry:
  def install(endpoint: ServerPacketEndpoint): Unit
  def uninstall(): Unit

object ServerPacketRuntimeRegistry extends RuntimeEndpointRegistry:
  override def install(endpoint: ServerPacketEndpoint): Unit = ServerPacketRuntime.install(endpoint)
  override def uninstall(): Unit = ServerPacketRuntime.uninstall()

/** 已构造服务器运行时的可观察生命周期；每次成功停服后由 coordinator 新建实例。 */
trait ServerRuntime:
  def onServerAboutToStart(event: FMLServerAboutToStartEvent): Unit = ()
  def onServerStarting(event: FMLServerStartingEvent): Unit
  def onServerStopping(event: FMLServerStoppingEvent): Unit = ()
  def onServerStopped(event: FMLServerStoppedEvent): Unit = ()

/** Stopping 阶段的累积结果；关闭 repository 留到 Stopped，避免错误地提前失去恢复证据。 */
final case class RuntimeStoppingReport(
    endpointUninstallFailure: Option[Throwable],
    persistenceReport: Option[PersistenceStoppingReport],
    persistenceFailure: Option[Throwable],
    startupAbortFailure: Option[Throwable] = None,
    deferredTermination: Option[RuntimeRecoveryTermination] = None,
    deferredFinishReport: Option[RecoveryFinishReport] = None,
    deferredFailure: Option[Throwable] = None,
    deferredUnregisterAttempted: Boolean = false,
    deferredUnregisterFailure: Option[Throwable] = None,
    controlledStopAttempted: Boolean = false,
    controlledStopSucceeded: Boolean = false,
    controlledStopFailure: Option[Throwable] = None
):
  require(endpointUninstallFailure != null && persistenceReport != null && persistenceFailure != null && startupAbortFailure != null && deferredTermination != null && deferredFinishReport != null && deferredFailure != null && deferredUnregisterFailure != null && controlledStopFailure != null, "runtime stopping report values must not be null")
  def isClean: Boolean = startupAbortFailure.isEmpty && endpointUninstallFailure.isEmpty && persistenceFailure.isEmpty && persistenceReport.forall(_.isClean) && deferredTermination.forall(_ == RuntimeRecoveryTermination.Published) && deferredFinishReport.forall(_.isClean) && deferredFailure.isEmpty
  def controlledRemediationPrepared: Boolean =
    deferredTermination.exists(_.isInstanceOf[RuntimeRecoveryTermination.PlannedRemediation]) &&
      deferredFinishReport.exists(report => report.exit == RecoveryExit.PlannedRemediation && report.isClean) &&
      deferredUnregisterAttempted && deferredUnregisterFailure.isEmpty &&
      controlledStopAttempted && controlledStopSucceeded && controlledStopFailure.isEmpty &&
      deferredFailure.isEmpty

/** Stopped 阶段唯一执行 repository.close 的不可变结果。 */
final case class FinalShutdownReport(
    stoppingReport: Option[RuntimeStoppingReport],
    closeFailure: Option[Throwable],
    resolverClearFailure: Option[Throwable],
    stoppingObserved: Boolean,
    deferredTermination: Option[RuntimeRecoveryTermination] = None,
    deferredFinishReport: Option[RecoveryFinishReport] = None,
    deferredFailure: Option[Throwable] = None
):
  require(stoppingReport != null && closeFailure != null && resolverClearFailure != null && deferredTermination != null && deferredFinishReport != null && deferredFailure != null, "final shutdown report values must not be null")
  def isClean: Boolean = stoppingObserved && stoppingReport.exists(_.isClean) && closeFailure.isEmpty && resolverClearFailure.isEmpty && deferredTermination.forall(_ == RuntimeRecoveryTermination.Published) && deferredFinishReport.forall(_.isClean)
  def dirtyClosed: Boolean = !isClean && closeFailure.isEmpty
  def controlledRemediationClosed: Boolean = stoppingObserved && stoppingReport.exists(report => report.controlledRemediationPrepared && report.endpointUninstallFailure.isEmpty && report.persistenceFailure.isEmpty) && closeFailure.isEmpty && resolverClearFailure.isEmpty

/** 串行管理 persistence、endpoint 与 repository 的启动回滚及停服清理。 */
final class ManagedServerRuntime(
    repository: FloatingPodRepository,
    persistence: RuntimePersistence,
    endpoint: ServerPacketEndpoint,
    endpointRegistry: RuntimeEndpointRegistry = ServerPacketRuntimeRegistry,
    ticketManager: Option[ChunkLoadingManager] = None,
    startupRecovery: Option[ServerStartupRecovery] = None,
    bindStartingEvent: FMLServerStartingEvent => Unit = _ => (),
    clearStartingEvent: () => Unit = () => (),
    validateWorldBinding: FMLServerAboutToStartEvent => Unit = _ => (),
    deferredRecovery: Option[DeferredRecoveryBundle] = None
) extends ServerRuntime:
  require(repository != null, "repository must not be null")
  require(persistence != null, "persistence must not be null")
  require(endpoint != null, "endpoint must not be null")
  require(endpointRegistry != null, "endpointRegistry must not be null")
  require(validateWorldBinding != null, "validateWorldBinding must not be null")
  require(startupRecovery.isEmpty || deferredRecovery.isEmpty, "legacy startup recovery and deferred recovery are mutually exclusive")

  private val logger = LogManager.getLogger(getClass)

  private var started = false
  private var persistenceStarted = false
  private var startupAborted = false
  private var bindingActive = false
  private var bindingCleared = false
  private var startupAbortFailure: Option[Throwable] = None
  private var callbackRegistered = false
  private var worldBindingValidated = false
  private var endpointInstalled = false
  private var deferredBegun = false
  private var deferredRecovering = false
  private var deferredEventsRegistered = false
  private var controlledStopRequested = false
  private var deferredTermination: Option[RuntimeRecoveryTermination] = None
  private var deferredFinishReport: Option[RecoveryFinishReport] = None
  private var deferredFailure: Option[Throwable] = None
  private var deferredFinishInvoked = false
  private var deferredUnregisterAttempted = false
  private var deferredUnregisterFailure: Option[Throwable] = None
  private var controlledStopFailure: Option[Throwable] = None
  private var controlledStopSucceeded = false
  private var startingEvent: FMLServerStartingEvent = null
  private var stoppingReport: Option[RuntimeStoppingReport] = None
  private var finalReport: Option[FinalShutdownReport] = None

  def latestStoppingReport: Option[RuntimeStoppingReport] = synchronized { stoppingReport }
  def latestFinalShutdownReport: Option[FinalShutdownReport] = synchronized { finalReport }

  override def onServerAboutToStart(event: FMLServerAboutToStartEvent): Unit = synchronized {
    if finalReport.nonEmpty then throw new IllegalStateException("Server runtime is already closed")
    if !worldBindingValidated then
      validateWorldBinding(event)
      worldBindingValidated = true
    if !callbackRegistered then
      ticketManager.foreach(_.registerRecoveryCallbackOnce())
      callbackRegistered = true
    deferredRecovery.foreach { bundle =>
      if !deferredEventsRegistered then
        try
          bundle.events.register(onDeferredTick)
          deferredEventsRegistered = true
        catch
          case primary: Throwable =>
            // A listener may be installed before its registration method reports failure.
            deferredEventsRegistered = true
            unregisterDeferredEvents(Some(primary))
            throw primary
    }
  }

  override def onServerStarting(event: FMLServerStartingEvent): Unit = synchronized {
    if started then throw new IllegalStateException("Server runtime is already started")
    if startupAborted then throw new IllegalStateException("Server runtime startup was already aborted")
    if finalReport.nonEmpty then throw new IllegalStateException("Server runtime is already closed")
    if ticketManager.nonEmpty && !callbackRegistered then throw new IllegalStateException("Ticket recovery callback must be registered before server starting")

    var endpointAttempted = false
    try
      bindingActive = true
      bindStartingEvent(event)
      startingEvent = event
      deferredRecovery match
        case Some(bundle) =>
          deferredBegun = true
          bundle.recovery.begin() match
            case RecoveryStep.AwaitingTick => deferredRecovering = true
            case RecoveryStep.Terminal(decision) => handleDeferredTerminal(bundle, decision)
        case None =>
          startupRecovery.foreach { recovery =>
            val report = recovery.recover()
            if !report.isClean then throw report.failure.getOrElse(new IllegalStateException("Startup recovery is not clean"))
          }
          persistence.start(event)
          persistenceStarted = true
          endpointAttempted = true
          endpointRegistry.install(endpoint)
          endpointInstalled = true
    catch
      case primary: Throwable =>
        abortStart(primary, endpointAttempted)
        throw primary
    if !startupAborted && deferredTermination.forall(_ == RuntimeRecoveryTermination.Published) then started = true
  }

  override def onServerStopping(event: FMLServerStoppingEvent): Unit = synchronized {
    if finalReport.nonEmpty || stoppingReport.nonEmpty then return
    if deferredEventsRegistered && !deferredBegun then
      deferredTermination = Some(RuntimeRecoveryTermination.PreparedStopBeforeStarting)
      unregisterDeferredEvents(None)
      stoppingReport = Some(deferredStoppingReport())
      return
    if deferredRecovering then
      deferredTermination = Some(RuntimeRecoveryTermination.ExternalStopWhileRecovering)
      val terminalFailure = finishRecoveringDeferred(RecoveryExit.ExternalStopWhileRecovering)
      stoppingReport = Some(deferredStoppingReport())
      terminalFailure.foreach { failure => throw failure }
      return
    if deferredTermination.exists(_ != RuntimeRecoveryTermination.Published) then
      stoppingReport = Some(deferredStoppingReport())
      return
    if startupAborted then
      stoppingReport = Some(deferredStoppingReport())
      return
    if !started then
      val primary = new IllegalStateException("Server stopped before startup completed")
      clearBinding().foreach(primary.addSuppressed)
      startupAborted = true
      startupAbortFailure = Some(primary)
      stoppingReport = Some(RuntimeStoppingReport(None, None, None, Some(primary), deferredTermination, deferredFinishReport, deferredFailure))
      return
    var uninstallFailure: Option[Throwable] = None
    var report: Option[PersistenceStoppingReport] = None
    var persistenceFailure: Option[Throwable] = None
    if endpointInstalled then
      try endpointRegistry.uninstall()
      catch case failure: Throwable => uninstallFailure = Some(failure)
      endpointInstalled = false
    try report = persistence.stopWithReport(event)
    catch case failure: Throwable => persistenceFailure = Some(failure)
    stoppingReport = Some(runtimeStoppingReport(uninstallFailure, report, persistenceFailure, None))
    started = false
  }

  private def onDeferredTick(phase: ServerRecoveryTickPhase): Unit = synchronized {
    if !deferredRecovering || finalReport.nonEmpty then return
    val bundle = deferredRecovery.getOrElse(throw new IllegalStateException("Deferred tick arrived without a deferred recovery bundle"))
    val step =
      try Some(bundle.recovery.advance(phase))
      catch
        case primary: Throwable =>
          deferredFailure = Some(primary)
          deferredTermination = Some(RuntimeRecoveryTermination.Aborted(primary))
          finishDeferred(RecoveryExit.AbortStartup, Some(primary))
          unregisterDeferredEvents(Some(primary))
          deferredRecovering = false
          started = false
          startupAborted = true
          startupAbortFailure = Some(primary)
          requestControlledStop(Some(primary))
          throw primary
    step.foreach {
      case RecoveryStep.AwaitingTick => ()
      case RecoveryStep.Terminal(decision) => handleDeferredTerminal(bundle, decision)
    }
  }

  private def publishDeferred(bundle: DeferredRecoveryBundle): Unit =
    var endpointAttempted = false
    try
      persistence.start(startingEvent)
      persistenceStarted = true
      endpointAttempted = true
      endpointRegistry.install(endpoint)
      endpointInstalled = true
      finishDeferred(RecoveryExit.Published, None)
      unregisterDeferredEvents(None)
      if deferredFailure.nonEmpty || deferredUnregisterFailure.nonEmpty then throw deferredFailure.orElse(deferredUnregisterFailure).get
      deferredTermination = Some(RuntimeRecoveryTermination.Published)
      deferredRecovering = false
    catch
      case primary: Throwable =>
        deferredFailure = Some(primary)
        abortStart(primary, endpointAttempted)
        requestControlledStop(Some(primary))
        throw primary

  private def handleDeferredTerminal(bundle: DeferredRecoveryBundle, decision: StartupPublicationDecision): Unit = decision match
    case StartupPublicationDecision.Permit(_) => publishDeferred(bundle)
    case StartupPublicationDecision.RemediationRequiresFreshBoot(podUuid) =>
      terminateDeferredWithControlledStop(RecoveryExit.PlannedRemediation, RuntimeRecoveryTermination.PlannedRemediation(podUuid))
    case StartupPublicationDecision.Block(reason) =>
      terminateDeferredWithControlledStop(RecoveryExit.Blocked, RuntimeRecoveryTermination.Blocked(reason, None))
    case StartupPublicationDecision.Failed(reason, cause) =>
      terminateDeferredWithControlledStop(RecoveryExit.Blocked, RuntimeRecoveryTermination.Blocked(reason, Some(cause)))

  /** 终端拒绝先完成 cleanup 并记录一次诊断，再请求受控停服；所有边界最多执行一次。 */
  private def terminateDeferredWithControlledStop(exit: RecoveryExit, termination: RuntimeRecoveryTermination): Unit =
    deferredTermination = Some(termination)
    val recoveryCause = termination match
      case RuntimeRecoveryTermination.Blocked(_, cause) => cause
      case _ => None
    val finishFailure =
      try
        finishDeferred(exit, recoveryCause)
        None
      catch case failure: Throwable => Some(failure)
    val finishPrimary = recoveryCause.orElse(finishFailure)
    val unregisterFailure = unregisterDeferredEvents(finishPrimary)
    deferredRecovering = false
    started = false
    val primary = finishPrimary.orElse(unregisterFailure)
    logDeferredTerminal(termination, primary)
    val stopFailure =
      try
        requestControlledStop(primary)
        None
      catch case failure: Throwable => Some(failure)
    val terminalFailure = primary.orElse(stopFailure)
    if controlledStopFailure.nonEmpty then terminalFailure.foreach(logControlledStopFailure)
    terminalFailure.foreach { failure =>
      deferredFailure = Some(failure)
      if recoveryCause.isEmpty || controlledStopFailure.nonEmpty then throw failure
    }

  private def logDeferredTerminal(termination: RuntimeRecoveryTermination, primary: Option[Throwable]): Unit = termination match
    case RuntimeRecoveryTermination.Blocked(reason, Some(cause)) =>
      logger.log(Level.ERROR, s"FloatingPod startup recovery failed before endpoint publication: $reason", cause)
    case RuntimeRecoveryTermination.Blocked(reason, None) =>
      primary match
        case Some(cause) => logger.log(Level.ERROR, s"FloatingPod startup recovery blocked before endpoint publication: $reason", cause)
        case None => logger.log(Level.ERROR, s"FloatingPod startup recovery blocked before endpoint publication: $reason")
    case _ => ()

  private def logControlledStopFailure(primary: Throwable): Unit =
    logger.log(Level.ERROR, "FloatingPod controlled stop request failed after startup recovery termination", primary)

  /** 完成 still-recovering 分支的 cleanup；调用方负责在报告发布后决定是否传播聚合失败。 */
  private def finishRecoveringDeferred(exit: RecoveryExit): Option[Throwable] =
    val finishFailure =
      try
        finishDeferred(exit, None)
        None
      catch case failure: Throwable => Some(failure)
    val unregisterFailure = unregisterDeferredEvents(finishFailure)
    deferredRecovering = false
    started = false
    finishFailure.orElse(unregisterFailure)

  private def finishDeferred(exit: RecoveryExit, primary: Option[Throwable]): Unit =
    if deferredFinishInvoked then return
    val bundle = deferredRecovery.getOrElse(throw new IllegalStateException("Deferred recovery finish requested without a bundle"))
    deferredFinishInvoked = true
    try
      val report = bundle.recovery.finish(exit)
      deferredFinishReport = Some(report)
      if report.exit != exit || !report.isClean then
        val failure = new IllegalStateException(s"Deferred recovery finish report is not clean or does not match $exit")
        report.cleanupFailures.foreach(failure.addSuppressed)
        primary.foreach(_.addSuppressed(failure))
        deferredFailure = Some(primary.getOrElse(failure))
        if primary.isEmpty then throw failure
    catch
      case failure: Throwable =>
        primary match
          case Some(existing) if existing ne failure => existing.addSuppressed(failure)
          case _ =>
            deferredFailure = Some(failure)
            throw failure

  private def unregisterDeferredEvents(primary: Option[Throwable]): Option[Throwable] =
    if !deferredEventsRegistered then return None
    deferredEventsRegistered = false
    deferredUnregisterAttempted = true
    try
      deferredRecovery.foreach(_.events.unregister())
      None
    catch
      case failure: Throwable =>
        deferredUnregisterFailure = Some(failure)
        primary match
          case Some(existing) if existing ne failure => existing.addSuppressed(failure)
          case _ => deferredFailure = Some(failure)
        Some(failure)

  private def requestControlledStop(primary: Option[Throwable]): Unit =
    if controlledStopRequested then return
    controlledStopRequested = true
    try
      deferredRecovery.foreach(_.controlledStop.requestStop())
      controlledStopSucceeded = true
    catch
      case failure: Throwable =>
        controlledStopFailure = Some(failure)
        primary match
          case Some(existing) if existing ne failure => existing.addSuppressed(failure)
          case _ =>
            deferredFailure = Some(failure)
            throw failure

  private def deferredStoppingReport(): RuntimeStoppingReport =
    runtimeStoppingReport(None, None, None, startupAbortFailure)

  private def runtimeStoppingReport(
      endpointUninstallFailure: Option[Throwable],
      persistenceReport: Option[PersistenceStoppingReport],
      persistenceFailure: Option[Throwable],
      startupAbortFailure: Option[Throwable]
  ): RuntimeStoppingReport =
    RuntimeStoppingReport(
      endpointUninstallFailure,
      persistenceReport,
      persistenceFailure,
      startupAbortFailure,
      deferredTermination,
      deferredFinishReport,
      deferredFailure,
      deferredUnregisterAttempted = deferredUnregisterAttempted,
      deferredUnregisterFailure = deferredUnregisterFailure,
      controlledStopAttempted = controlledStopRequested,
      controlledStopSucceeded = controlledStopSucceeded,
      controlledStopFailure = controlledStopFailure
    )

  private def abortStart(primary: Throwable, endpointAttempted: Boolean = false): Unit =
    if deferredRecovery.nonEmpty then
      if deferredTermination.isEmpty then
        deferredTermination = Some(RuntimeRecoveryTermination.Aborted(primary))
      if deferredBegun then finishDeferred(RecoveryExit.AbortStartup, Some(primary))
      unregisterDeferredEvents(Some(primary))
      deferredRecovering = false
    if endpointInstalled || endpointAttempted then
      try endpointRegistry.uninstall()
      catch case cleanup: Throwable => primary.addSuppressed(cleanup)
      endpointInstalled = false
    if persistenceStarted then
      try persistence.abortStarted()
      catch case cleanup: Throwable => primary.addSuppressed(cleanup)
      persistenceStarted = false
    clearBinding().foreach(primary.addSuppressed)
    startupAborted = true
    startupAbortFailure = Some(primary)
    started = false

  private def clearBinding(): Option[Throwable] =
    if !bindingActive || bindingCleared then None
    else
      bindingCleared = true
      try
        clearStartingEvent()
        None
      catch case failure: Throwable => Some(failure)

  /** AboutToStart 已登记 deferred callback 时，即使尚未 bind 也必须清空 resolver 边界。 */
  private def clearPreparedDeferredBinding(): Option[Throwable] =
    if bindingCleared then None
    else
      bindingCleared = true
      try
        clearStartingEvent()
        None
      catch case failure: Throwable => Some(failure)

  override def onServerStopped(event: FMLServerStoppedEvent): Unit = synchronized {
    if finalReport.nonEmpty then return
    val preparedDeferredStopped = deferredEventsRegistered && !deferredBegun
    val preparedUnregisterFailure =
      if preparedDeferredStopped then
        deferredTermination = Some(RuntimeRecoveryTermination.PreparedStopBeforeStarting)
        unregisterDeferredEvents(None)
      else None
    val deferredTerminalFailure =
      if deferredRecovering then
        if deferredTermination.isEmpty then
          deferredTermination = Some(RuntimeRecoveryTermination.StoppedWithoutStopping)
        finishRecoveringDeferred(RecoveryExit.StoppedWithoutStopping)
      else None
    val shutdownFailure = preparedUnregisterFailure.orElse(deferredTerminalFailure)
    val closeFailure = try
      repository.close(); None
    catch case failure: Throwable => Some(failure)
    val resolverClearFailure =
      if preparedDeferredStopped then clearPreparedDeferredBinding()
      else clearBinding()
    finalReport = Some(FinalShutdownReport(stoppingReport, closeFailure, resolverClearFailure, stoppingReport.nonEmpty, deferredTermination, deferredFinishReport, deferredFailure))
    shutdownFailure.foreach { primary =>
      closeFailure.foreach(primary.addSuppressed)
      resolverClearFailure.foreach(primary.addSuppressed)
      throw primary
    }
  }

/** 创建已完整装配但尚未启动的服务器运行时。 */
trait ServerRuntimeFactory:
  def create(settings: ServerRuntimeSettings): ServerRuntime
  private[network] def createClaimed(server: MinecraftServer): ServerRuntime =
    throw new IllegalStateException("factory does not provide an actual claimed dedicated runtime")

/** 生产装配工厂；所有 server-private 资源只在服务端启动后按不可变设置创建。 */
final class ForgeServerRuntimeFactory(
    modInstance: Object,
    repositoryFactory: Path => FloatingPodRepository = path => new SQLiteFloatingPodRepository(path.toString),
    ticketManagerFactory: (FloatingPodRepository, Object) => ChunkLoadingManager = (repository, instance) => ChunkLoadingManager.forForge(repository, instance),
    managerFactory: (FloatingPodRepository, ChunkLoadingManager) => ActiveFloatingPodManager = (repository, tickets) => new ActiveFloatingPodManager(repository, tickets),
    flushFactory: ActiveFloatingPodManager => PeriodicFlushService = manager => new PeriodicFlushService(manager),
    persistenceFactory: (ActiveFloatingPodManager, PeriodicFlushService) => PersistenceManager = (manager, flush) => new PersistenceManager(manager, flush),
    endpointFactory: PodRequestProcessor => ServerPacketEndpoint = processor => new ForgeServerPacketEndpoint(ForgeServerPlayerContextFactory, processor, ForgeServerPacketSender()),
    endpointRegistry: RuntimeEndpointRegistry = ServerPacketRuntimeRegistry,
    worldBindingGateFactory: PhysicalRecoveryStore => (FMLServerAboutToStartEvent => Unit) = ForgeServerRuntimeFactory.defaultWorldBindingGateFactory,
    deferredBundleFactory: (FloatingPodRepository, ChunkLoadingManager, ActiveFloatingPodManager, ServerWorldResolver, () => MinecraftServer) => DeferredRecoveryBundle = ForgeServerRuntimeFactory.defaultDeferredBundleFactory
) extends ServerRuntimeFactory:
  require(modInstance != null, "modInstance must not be null")
  require(repositoryFactory != null, "repositoryFactory must not be null")
  require(ticketManagerFactory != null, "ticketManagerFactory must not be null")
  require(managerFactory != null, "managerFactory must not be null")
  require(flushFactory != null, "flushFactory must not be null")
  require(persistenceFactory != null, "persistenceFactory must not be null")
  require(endpointFactory != null, "endpointFactory must not be null")
  require(endpointRegistry != null, "endpointRegistry must not be null")
  require(worldBindingGateFactory != null, "worldBindingGateFactory must not be null")
  require(deferredBundleFactory != null, "deferredBundleFactory must not be null")

  /** 为 Java FML 入口保留不暴露 Scala Function 参数的生产构造器。 */
  def this(modInstance: Object) = this(
    modInstance,
    path => new SQLiteFloatingPodRepository(path.toString),
    (repository, instance) => ChunkLoadingManager.forForge(repository, instance),
    (repository, tickets) => new ActiveFloatingPodManager(repository, tickets),
    manager => new PeriodicFlushService(manager),
    (manager, flush) => new PersistenceManager(manager, flush),
    processor => new ForgeServerPacketEndpoint(ForgeServerPlayerContextFactory, processor, ForgeServerPacketSender()),
    ServerPacketRuntimeRegistry,
    ForgeServerRuntimeFactory.defaultWorldBindingGateFactory,
    ForgeServerRuntimeFactory.defaultDeferredBundleFactory
  )

  override private[network] def createClaimed(server: MinecraftServer): ServerRuntime =
    ProductionBootstrapInstallation.requireClaimedCallbacks(server) match
      case _:ProductionDbFirstBootstrapCallbacks=>DbFirstDedicatedRuntime.claimed(server,modInstance)
      case _:ProductionDedicatedBootstrapCallbacks=>DedicatedProjectionRuntime.claimed(server,modInstance)
      case _=>throw new IllegalStateException("actual production callback family required for dedicated runtime")

  override def create(settings: ServerRuntimeSettings): ServerRuntime =
    throw new IllegalStateException("Legacy SQLite production runtime is disabled; MySQL requires a claimed dedicated runtime")

  /** 仅供历史装配回归；生产入口不得调用旧 SQLite 装配。 */
  private[network] def createLegacyForTests(settings: ServerRuntimeSettings): ServerRuntime =
    require(settings != null, "settings must not be null")
    var repository: FloatingPodRepository = null
    try
      val databaseDirectory = settings.sqlitePath.getParent
      if databaseDirectory == null then throw new IllegalArgumentException("SQLite path must have a parent directory")
      Files.createDirectories(databaseDirectory)
      repository = repositoryFactory(settings.sqlitePath)
      if repository == null then throw new IllegalStateException("Repository factory returned null")
      val (recoveryStore, entitlementStore, physicalRecoveryStore) = requireS5Stores(repository)
      val validateWorldBinding = worldBindingGateFactory(physicalRecoveryStore)
      if validateWorldBinding == null then throw new IllegalStateException("World binding gate factory returned null")
      val tickets = ticketManagerFactory(repository, modInstance)
      if tickets == null then throw new IllegalStateException("Chunk ticket manager factory returned null")
      val manager = managerFactory(repository, tickets)
      if manager == null then throw new IllegalStateException("Active manager factory returned null")
      val capacity = new ActiveCapacityPolicy(recoveryStore, settings.activeLimit)
      val processor = new PodRequestProcessor(
        new OwnershipValidator(repository),
        capacity,
        new PositionValidator(),
        new PersistedPlayerEntitlementPolicy(entitlementStore, recoveryStore),
        new ActivePodLifecyclePort(manager, settings.activeLimit),
        Log4jRequestLog()
      )
      val endpoint = endpointFactory(processor)
      val flush = flushFactory(manager)
      val persistence = persistenceFactory(manager, flush)
      var runtimeServer: Option[MinecraftServer] = None
      val worlds = new ServerWorldResolver:
        override def resolve(dimensionId: Int) =
          runtimeServer.flatMap(server => Option(server.getWorld(dimensionId)).map(world => PodWorldImpl(world)))
      val serverSupplier = () => runtimeServer.orNull
      val deferredBundle = deferredBundleFactory(repository, tickets, manager, worlds, serverSupplier)
      if deferredBundle == null then throw new IllegalStateException("Deferred recovery bundle factory returned null")
      new ManagedServerRuntime(
        repository,
        new PersistenceManagerRuntime(persistence),
        endpoint,
        endpointRegistry,
        Some(tickets),
        None,
        event => runtimeServer = Option(event).flatMap(value => Option(value.getServer)),
        () => runtimeServer = None,
        validateWorldBinding,
        Some(deferredBundle)
      )
    catch
      case primary: Throwable =>
        if repository != null then
          try repository.close()
          catch case closeFailure: Throwable => primary.addSuppressed(closeFailure)
        throw primary

  private def requireS5Stores(repository: FloatingPodRepository): (RecoveryPodStore, PlayerEntitlementStore, PhysicalRecoveryStore) = repository match
    case _ if repository.isInstanceOf[ActivityTransitionStore] && repository.isInstanceOf[RecoveryPodStore] && repository.isInstanceOf[PlayerEntitlementStore] && repository.isInstanceOf[PhysicalRecoveryStore] =>
      (repository.asInstanceOf[RecoveryPodStore], repository.asInstanceOf[PlayerEntitlementStore], repository.asInstanceOf[PhysicalRecoveryStore])
    case _ => throw new IllegalStateException("S5 runtime requires ActivityTransitionStore, RecoveryPodStore, PlayerEntitlementStore, and PhysicalRecoveryStore")

object ForgeServerRuntimeFactory:
  /**
   * 为一个已接纳的 physical recovery store 构造 AboutToStart binding validator。
   * 本世界首次启动（尚无 binding）时先完成一次 world binding cutover，随后仍由严格只读 gate 校验；
   * 已存在 binding 时 bootstrap 不介入，gate 语义完全不变。
   */
  val defaultWorldBindingGateFactory: PhysicalRecoveryStore => (FMLServerAboutToStartEvent => Unit) = store =>
    val gate = new CertifiedWorldBindingGate(store)
    event =>
      WorldBindingBootstrap.ensureCertifiedBinding(store, event)
      gate.validate(event)
      ()

  val defaultDeferredBundleFactory: (FloatingPodRepository, ChunkLoadingManager, ActiveFloatingPodManager, ServerWorldResolver, () => MinecraftServer) => DeferredRecoveryBundle =
    (repository, tickets, manager, worlds, serverSupplier) => LiveCrashRecovery.productionBundle(repository, tickets, manager, worlds, serverSupplier)

/** 历史运行时测试装配入口；正常 FML 生命周期使用公开构造器。 */
object ServerRuntimeLifecycle:
  /** 仅保留旧运行时的隔离行为回归，不代表 production 支持 SQLite。 */
  private[floatingpod] def legacyForTests(factory: ServerRuntimeFactory): ServerRuntimeLifecycle =
    new ServerRuntimeLifecycle(factory, true)

/** 约束 server start 与 stop 顺序；production 仅接纳 dedicated MySQL runtime。 */
final class ServerRuntimeLifecycle private (factory: ServerRuntimeFactory, legacyTestMode: Boolean):
  def this(factory: ServerRuntimeFactory) = this(factory, false)
  require(factory != null, "factory must not be null")

  private var settings: Option[ServerRuntimeSettings] = None
  private var activeRuntime: Option[ServerRuntime] = None
  private var runtimeStarted = false
  private var lastStoppingReport: Option[RuntimeStoppingReport] = None
  private var lastFinalShutdownReport: Option[FinalShutdownReport] = None

  def latestStoppingReport: Option[RuntimeStoppingReport] = synchronized { lastStoppingReport }
  def latestFinalShutdownReport: Option[FinalShutdownReport] = synchronized { lastFinalShutdownReport }

  /** SQLite/FML settings loader is isolated to the explicit legacy-test fixture; production uses managed MySQL early config. */
  def configure(suggestedConfigurationFile: File): Unit =
    require(suggestedConfigurationFile != null, "suggested configuration file must not be null")
    if !legacyTestMode then
      throw new IllegalStateException("Production server lifecycle does not load SQLite settings; use the fixed managed database configuration")
    configure(ServerRuntimeSettings.load(suggestedConfigurationFile))

  def configure(configuredSettings: ServerRuntimeSettings): Unit = synchronized {
    require(configuredSettings != null, "configuredSettings must not be null")
    if settings.nonEmpty then throw new IllegalStateException("Server runtime lifecycle is already configured")
    if activeRuntime.nonEmpty then throw new IllegalStateException("Cannot configure an active server runtime lifecycle")
    settings = Some(configuredSettings)
  }

  def onServerAboutToStart(event: FMLServerAboutToStartEvent): Unit = synchronized {
    if activeRuntime.nonEmpty then throw new IllegalStateException("Server runtime lifecycle is already started")
    lastStoppingReport = None
    lastFinalShutdownReport = None
    val runtime = if legacyTestMode then
      factory.create(settings.getOrElse(throw new IllegalStateException("Server runtime lifecycle is not configured")))
    else
      val server = Option(event).flatMap(value => Option(value.getServer)).getOrElse(
        throw new IllegalArgumentException("actual server required at AboutToStart"))
      if !server.isDedicatedServer then
        throw new IllegalStateException("Integrated MySQL runtime is not provided; refusing legacy SQLite runtime before resource creation")
      factory.createClaimed(server)
    activeRuntime = Some(runtime)
    runtimeStarted = false
    try runtime.onServerAboutToStart(event)
    catch
      case primary: Throwable =>
        try runtime.onServerStopped(null)
        catch case cleanup: Throwable => ManagedSourceSaveLedger.suppressSecondary(primary, cleanup)
        captureReports(runtime)
        lastFinalShutdownReport.foreach { report =>
          report.closeFailure.foreach(cleanup => ManagedSourceSaveLedger.suppressSecondary(primary, cleanup))
          report.resolverClearFailure.foreach(cleanup => ManagedSourceSaveLedger.suppressSecondary(primary, cleanup))
        }
        activeRuntime = None
        runtimeStarted = false
        throw primary
  }

  def onServerStarting(event: FMLServerStartingEvent): Unit = synchronized {
    val runtime = activeRuntime.getOrElse(throw new IllegalStateException("Server runtime lifecycle has not reached about-to-start"))
    if runtimeStarted then throw new IllegalStateException("Server runtime lifecycle is already started")
    runtime.onServerStarting(event)
    runtimeStarted = true
  }

  def onServerStopping(event: FMLServerStoppingEvent): Unit = synchronized {
    val runtime = activeRuntime.getOrElse(throw new IllegalStateException("Server runtime lifecycle is not started"))
    try runtime.onServerStopping(event)
    finally captureReports(runtime)
  }

  def onServerStopped(event: FMLServerStoppedEvent): Unit = synchronized {
    activeRuntime.foreach { runtime =>
      try runtime.onServerStopped(event)
      finally
        captureReports(runtime)
        activeRuntime = None
        runtimeStarted = false
    }
  }

  private def captureReports(runtime: ServerRuntime): Unit = runtime match
    case managed: ManagedServerRuntime =>
      lastStoppingReport = managed.latestStoppingReport.orElse(lastStoppingReport)
      lastFinalShutdownReport = managed.latestFinalShutdownReport.orElse(lastFinalShutdownReport)
    case _ => ()
