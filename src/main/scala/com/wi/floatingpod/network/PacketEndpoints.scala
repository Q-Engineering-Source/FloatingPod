package com.wi.floatingpod.network

import net.minecraftforge.fml.common.FMLCommonHandler
import net.minecraftforge.fml.common.network.simpleimpl.{IMessage, IMessageHandler, MessageContext}

/** 将网络回调切换到对应 FML 主线程的可替换边界。 */
trait MainThreadScheduler:
  def schedule(context: MessageContext, work: Runnable): Unit

/** 真实 Forge 主线程调度器；仅在已收到网络消息后使用 context。 */
object ForgeMainThreadScheduler extends MainThreadScheduler:
  override def schedule(context: MessageContext, work: Runnable): Unit =
    require(context != null, "context must not be null")
    require(work != null, "work must not be null")
    FMLCommonHandler.instance().getWorldThread(context.netHandler).addScheduledTask(work)

/** 后续 server runtime 必须实现的最小请求入口。 */
trait ServerPacketEndpoint:
  def receiveRelease(message: C2SReleaseRequest, context: MessageContext): Unit
  def receiveRecall(message: C2SRecallRequest, context: MessageContext): Unit
  def receiveRecallOperation(message: C2SRecallOperationRequest, context: MessageContext): Unit =
    throw new IllegalStateException("Correlated recall endpoint is not installed")

/** 未安装运行时 processor 时的默认入口，任务执行时立即失败而不改变状态。 */
object UnavailableServerPacketEndpoint extends ServerPacketEndpoint:
  override def receiveRelease(message: C2SReleaseRequest, context: MessageContext): Unit = throw new IllegalStateException("No server packet endpoint is installed")
  override def receiveRecall(message: C2SRecallRequest, context: MessageContext): Unit = throw new IllegalStateException("No server packet endpoint is installed")

/**
 * 供公共 C2S handler 在服务器装配前后动态解析的运行时入口。
 *
 * 已注册的 handler 永远指向本对象，而不是捕获一次性的未安装 endpoint；这样服务器生命周期可在
 * 不重注册协议 discriminator 的前提下安装或卸载真正的处理器。
 */
object ServerPacketRuntime extends ServerPacketEndpoint:
  @volatile private var installed: Option[ServerPacketEndpoint] = None

  /** 安装唯一的服务器请求入口；重复安装必须由生命周期装配方显式处理。 */
  def install(endpoint: ServerPacketEndpoint): Unit = synchronized {
    require(endpoint != null, "endpoint must not be null")
    if installed.nonEmpty then throw new IllegalStateException("A server packet endpoint is already installed")
    installed = Some(endpoint)
  }

  /** 卸载当前入口；后续已排程请求会再次以 fail-closed 语义解析。 */
  def uninstall(): Unit = synchronized { installed = None }

  /** schema6 owner只撤自己的入口；失败清理不得卸掉后继或其他runtime。 */
  private[floatingpod] def uninstall(endpoint: ServerPacketEndpoint): Unit = synchronized {
    require(endpoint != null && installed.exists(_ eq endpoint), "original installed packet endpoint required")
    installed = None
  }

  override def receiveRelease(message: C2SReleaseRequest, context: MessageContext): Unit = current.receiveRelease(message, context)
  override def receiveRecall(message: C2SRecallRequest, context: MessageContext): Unit = current.receiveRecall(message, context)
  override def receiveRecallOperation(message: C2SRecallOperationRequest, context: MessageContext): Unit = current.receiveRecallOperation(message, context)

  private def current: ServerPacketEndpoint = installed.getOrElse(UnavailableServerPacketEndpoint)

/** 客户端网络快照的最小公共安全接收边界，不包含 GUI 或渲染调用。 */
trait ClientPacketSink:
  def receiveStateSync(message: S2CStateSync): Unit
  def receiveReleaseResult(message: S2CReleaseResult): Unit
  def receiveRecallResult(message: S2CRecallResult): Unit

/** 保存最近网络消息的公共安全 sink，供未来客户端装配按需读取。 */
object ClientNetworkState extends ClientPacketSink:
  private var recallStatusConsumer: S2CRecallOperationStatus => Unit = _ =>
    throw new IllegalStateException("Actual client recall consumer is not installed")
  private[floatingpod] def installRecallStatusConsumer(consumer: S2CRecallOperationStatus => Unit): Unit =
    require(consumer != null, "actual client recall consumer required")
    recallStatusConsumer = consumer
  private[network] def receiveRecallOperationStatus(message: S2CRecallOperationStatus): Unit = recallStatusConsumer(message)
  @volatile var latestStateSync: Option[S2CStateSync] = None
  @volatile var latestReleaseResult: Option[S2CReleaseResult] = None
  @volatile var latestRecallResult: Option[S2CRecallResult] = None
  override def receiveStateSync(message: S2CStateSync): Unit = latestStateSync = Some(message)
  override def receiveReleaseResult(message: S2CReleaseResult): Unit = latestReleaseResult = Some(message)
  override def receiveRecallResult(message: S2CRecallResult): Unit = latestRecallResult = Some(message)

/** 延迟 C2S Release endpoint 调用，确保它只发生在排程任务中。 */
final class C2SReleaseHandler(scheduler: MainThreadScheduler, endpoint: ServerPacketEndpoint) extends IMessageHandler[C2SReleaseRequest, IMessage]:
  def this() = this(ForgeMainThreadScheduler, ServerPacketRuntime)
  override def onMessage(message: C2SReleaseRequest, context: MessageContext): IMessage =
    scheduler.schedule(context, new Runnable { override def run(): Unit = endpoint.receiveRelease(message, context) })
    null

/** 延迟 C2S Recall endpoint 调用，确保它只发生在排程任务中。 */
final class C2SRecallHandler(scheduler: MainThreadScheduler, endpoint: ServerPacketEndpoint) extends IMessageHandler[C2SRecallRequest, IMessage]:
  def this() = this(ForgeMainThreadScheduler, ServerPacketRuntime)
  override def onMessage(message: C2SRecallRequest, context: MessageContext): IMessage =
    scheduler.schedule(context, new Runnable { override def run(): Unit = endpoint.receiveRecall(message, context) })
    null

final class C2SRecallOperationHandler(scheduler: MainThreadScheduler, endpoint: ServerPacketEndpoint) extends IMessageHandler[C2SRecallOperationRequest, IMessage]:
  override def onMessage(message: C2SRecallOperationRequest, context: MessageContext): IMessage =
    scheduler.schedule(context, new Runnable { override def run(): Unit = endpoint.receiveRecallOperation(message, context) })
    null

final class S2CRecallOperationStatusHandler extends IMessageHandler[S2CRecallOperationStatus, IMessage]:
  override def onMessage(message: S2CRecallOperationStatus, context: MessageContext): IMessage =
    ForgeMainThreadScheduler.schedule(context, new Runnable {
      override def run(): Unit = ClientNetworkState.receiveRecallOperationStatus(message)
    })
    null

/** 延迟 S2C StateSync sink 更新。 */
final class S2CStateSyncHandler(scheduler: MainThreadScheduler, sink: ClientPacketSink) extends IMessageHandler[S2CStateSync, IMessage]:
  def this() = this(ForgeMainThreadScheduler, ClientNetworkState)
  override def onMessage(message: S2CStateSync, context: MessageContext): IMessage =
    scheduler.schedule(context, new Runnable { override def run(): Unit = sink.receiveStateSync(message) })
    null

/** 延迟 S2C Release result sink 更新。 */
final class S2CReleaseResultHandler(scheduler: MainThreadScheduler, sink: ClientPacketSink) extends IMessageHandler[S2CReleaseResult, IMessage]:
  def this() = this(ForgeMainThreadScheduler, ClientNetworkState)
  override def onMessage(message: S2CReleaseResult, context: MessageContext): IMessage =
    scheduler.schedule(context, new Runnable { override def run(): Unit = sink.receiveReleaseResult(message) })
    null

/** 延迟 S2C Recall result sink 更新。 */
final class S2CRecallResultHandler(scheduler: MainThreadScheduler, sink: ClientPacketSink) extends IMessageHandler[S2CRecallResult, IMessage]:
  def this() = this(ForgeMainThreadScheduler, ClientNetworkState)
  override def onMessage(message: S2CRecallResult, context: MessageContext): IMessage =
    scheduler.schedule(context, new Runnable { override def run(): Unit = sink.receiveRecallResult(message) })
    null
