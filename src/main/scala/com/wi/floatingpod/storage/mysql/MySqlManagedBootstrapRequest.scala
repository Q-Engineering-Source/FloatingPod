package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.bootstrap.{EarlyDependencyResolver, ManagedDatabaseConfiguration, ProductionBootstrapControl}

import java.nio.file.Path

/**
 * 在原启动控制线程与专属 SQL I/O 线程之间传递一次性请求。
 * 构造时只登记不可变请求数据；文件系统、journal、JDBC 与读取预算 owner 均由首个实际 I/O 线程任务创建。
 */
private[floatingpod] final class MySqlManagedBootstrapRequest(
    configuration: ManagedDatabaseConfiguration,
    worldPath: Path,
    val identity: ProductionBootstrapControl.DbFirstIdentity,
    val policy: ProductionBootstrapControl.Policy,
    dependencyPins: EarlyDependencyResolver.ExpectedPins
):
  require(configuration != null && worldPath != null && identity != null && policy != null && dependencyPins != null,
    "complete managed database bootstrap request required")
  require(worldPath.isAbsolute, "managed world path must be absolute")

  private val controlThread = Thread.currentThread()
  private var workerThread: Thread = null
  private var attempted = false
  private var completed: ProductionBootstrapControl.DbFirstControl = null
  private var terminalFailure: Throwable = null
  private var installation: MySqlManagedInstallation = null
  private var closeAttempted = false

  /** 由原 adapter 的首个 owned-I/O 任务调用，且只允许一次。 */
  def prepareOnOwnedIoThread(): ProductionBootstrapControl.DbFirstControl = synchronized {
    val current = Thread.currentThread()
    if current eq controlThread then
      throw new IllegalStateException("managed initialization must begin on the dedicated owned-I/O worker")
    if workerThread == null then workerThread = current
    else if workerThread ne current then
      throw new IllegalStateException("managed initialization request requires its first exact owned-I/O worker")
    if attempted then
      terminalFailure match
        case null => throw new IllegalStateException("managed initialization request is one-shot")
        case previous => throw new IllegalStateException("managed initialization already failed; inspect its retained cause", previous)
    if closeAttempted then throw new IllegalStateException("managed initialization was closed before preparation")
    attempted = true
    installation = new MySqlManagedInstallation(configuration, worldPath, identity, policy, dependencyPins)
    try
      completed = installation.prepareOnOwnedIoThread()
      completed
    catch
      case failure: Throwable =>
        terminalFailure = failure
        installation.cleanupAfterFailure(failure)
        throw failure
  }

  def preparedControl(): ProductionBootstrapControl.DbFirstControl = synchronized {
    val current = Thread.currentThread()
    if (current ne controlThread) && (workerThread == null || (current ne workerThread)) then
      throw new IllegalStateException("managed control readback requires its registered control or owned-I/O thread")
    if completed == null then
      terminalFailure match
        case null => throw new IllegalStateException("managed initialization has not completed")
        case cause => throw new IllegalStateException("managed initialization did not produce a control", cause)
    completed
  }

  /** 清理必须留在已登记的 owned worker，并保留所有尚未解除责任的句柄。 */
  def closeOnOwnedIoThread(): Unit = synchronized {
    if workerThread == null then
      if Thread.currentThread() eq controlThread then
        throw new IllegalStateException("managed cleanup must run on the dedicated owned-I/O worker")
      workerThread = Thread.currentThread()
    requireWorker()
    if closeAttempted then
      if installation != null && installation.resourcesRetained then
        throw new IllegalStateException("managed initialization cleanup remains unresolved", terminalFailure)
      return
    closeAttempted = true
    if installation != null then
      try installation.closeOnOwnedIoThread()
      catch
        case cleanup: Throwable =>
          if terminalFailure == null then terminalFailure = cleanup
          else if cleanup ne terminalFailure then terminalFailure.addSuppressed(cleanup)
          throw cleanup
    else configuration.close()
  }

  def resourcesRetained: Boolean = synchronized {
    installation != null && installation.resourcesRetained
  }

  def failure: Option[Throwable] = synchronized(Option(terminalFailure))

  private def requireWorker(): Unit =
    if workerThread == null || (Thread.currentThread() ne workerThread) then
      throw new IllegalStateException("managed initialization cleanup and readback require its exact owned-I/O worker")
