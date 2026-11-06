package com.wi.floatingpod.bootstrap

import com.wi.floatingpod.server.io.{IoCompletion, IoOperationTag, IoSubmission, OwnedSerialIo}
import java.time.Duration
import java.util.concurrent.{CompletableFuture, CompletionStage, CountDownLatch, TimeUnit, TimeoutException}
import scala.util.{Failure, Success}

enum IoPreparationState:
  case NotSubmitted
  case Accepted
  case NotAccepted(submission: IoSubmission)
  case PendingDeadline(originalCause: Throwable)
  case ConsumedSuccess
  case ConsumedFailure(originalCause: Throwable)

final case class IoPreparationObservation(tag: IoOperationTag, state: IoPreparationState,
                                          startupFailure: Option[Throwable], unresolved: Boolean)

/** off-game control上的同步有界pump；stage只在真实mailbox消费之后完成。
  * 无新增线程、用户callback、游戏对象或资源能力。deadline/interrupt不取消已接受work。
  */
final class OwnedIoPreparation[C, A] private[bootstrap] (io: OwnedSerialIo[C, A], signal: CountDownLatch,
                                    exactControl: Thread,
                                    val tag: IoOperationTag, commandInput: C):
  require(io != null && signal != null && io.ownsCompletionSignal(signal), "actual IO completion signal required")
  require(signal.getCount == 1L, "fresh one-shot preparation completion signal required")
  require(exactControl != null, "actual bootstrap control required")
  require(tag != null, "exact preparation tag required")
  private var commandBacking: C | Null = commandInput
  private val future = new CompletableFuture[A]()
  private val readonlyStage = future.minimalCompletionStage()
  @volatile private var state: IoPreparationState = IoPreparationState.NotSubmitted
  @volatile private var startupFailure = Option.empty[Throwable]
  private var accepted = false
  private var consumed = false
  @volatile private var actualCompletion = Option.empty[IoCompletion[A]]

  def stage: CompletionStage[A] = readonlyStage
  def observation: IoPreparationObservation =
    IoPreparationObservation(tag, state, startupFailure, state match
      case IoPreparationState.Accepted | IoPreparationState.PendingDeadline(_) => true
      case _ => false)
  def successfulCompletionConsumed: Boolean = state == IoPreparationState.ConsumedSuccess && startupFailure.isEmpty
  def transferPermitted: Boolean = successfulCompletionConsumed && (commandBacking != null) && actualCompletion.nonEmpty
  /** 只检查原成功实物；真正退休还须外层 exact claim/control 终结链。 */
  private[floatingpod] def requireLiveApplicationValue(): A =
    require(transferPermitted && future.isDone && !future.isCompletedExceptionally && !future.isCancelled,
      "original live successful preparation required")
    val value = future.resultNow()
    actualCompletion.get.result match
      case Success(actual) => require(actual.asInstanceOf[AnyRef] eq value.asInstanceOf[AnyRef], "original successful completion value differs")
      case Failure(cause) => throw cause
    value
  /** 复用原control线程身份，不新增Bridge持有字段。 */
  private[floatingpod] def requireOriginalControl(actualBridge: DedicatedBootstrapBridge): Unit =
    require(actualBridge != null && (actualBridge.controlThread() eq exactControl), "original bootstrap control required")
  private[floatingpod] def retireConsumedApplicationValue(control: DedicatedBootstrapBridge,
      callbacks: DedicatedBootstrapBridge.Callbacks, receipt: Object, expected: A): Unit =
    requireOriginalControl(control)
    control.requireOrdinaryApplicationPreparation(callbacks, receipt, expected.asInstanceOf[AnyRef])
    val value = requireLiveApplicationValue()
    require(value.asInstanceOf[AnyRef] eq expected.asInstanceOf[AnyRef], "original application value required")
    commandBacking = null
    actualCompletion = None
  private[floatingpod] def requireApplicationValueRetired(): Unit =
    require(successfulCompletionConsumed && commandBacking == null && actualCompletion.isEmpty,
      "preparation application references remain retained")
  /** 保留消费实物，异常tag也不被静默丢弃；不声明DB/Ready/IO exit。 */
  def consumedCompletion: Option[IoCompletion[A]] = actualCompletion

  private def requireControl(): Unit =
    require(Thread.currentThread() eq exactControl, "only exact initial bootstrap control may pump preparation")
    io.requirePreparationControl(exactControl)

  private def nanos(limit: Duration): Long =
    require(limit != null && !limit.isNegative && !limit.isZero, "explicit finite positive preparation limit required")
    val result = limit.toNanos // overflow explicitly fails before work submission.
    require(result > 0L, "positive preparation nanos required")
    result

  def prepare(limit: Duration): CompletionStage[A] =
    requireControl()
    val budget = nanos(limit)
    require(state == IoPreparationState.NotSubmitted, "preparation submission is one-shot")
    io.submitPreparation(tag, commandBacking.asInstanceOf[C]) match
      case IoSubmission.Accepted =>
        accepted = true
        state = IoPreparationState.Accepted
        pump(budget)
      case rejection =>
        state = IoPreparationState.NotAccepted(rejection)
        val cause = rejection match
          case IoSubmission.RejectedStopped(Some(original)) => original
          case _ => new IllegalStateException(s"preparation submission was not accepted: $rejection")
        startupFailure = Some(cause)
        future.completeExceptionally(cause)
    readonlyStage

  /** 同owner的late收口；返回仅说明completion消费状态，不是resourceclose证明。 */
  def finalizePreparation(limit: Duration): IoPreparationObservation =
    requireControl()
    require(commandBacking != null, "retired preparation cannot finalize again")
    val budget = nanos(limit)
    io.quiesce()
    if accepted && !consumed then pump(budget)
    observation

  private def pump(budget: Long): Unit =
    try
      if !signal.await(budget, TimeUnit.NANOSECONDS) then
        throw new TimeoutException("accepted preparation completion remains unconsumed at deadline")
      val messages = io.drain(1)
      if messages.size != 1 then
        io.checkpoint.fatalCause.foreach(throw _)
        throw new IllegalStateException("completion signal did not carry the accepted preparation mailbox")
      val message = messages.head
      actualCompletion = Some(message)
      consumed = true // reservation已经由真实drain释放；保留完成实物后才publish stage。
      if message.tag != tag then
        val mismatch = new IllegalStateException("preparation mailbox carried a different exact operation tag")
        state = IoPreparationState.ConsumedFailure(mismatch)
        startupFailure = startupFailure.orElse(Some(mismatch))
        future.completeExceptionally(mismatch)
        throw mismatch
      message.result match
        case Success(value) =>
          state = IoPreparationState.ConsumedSuccess
          future.complete(value)
        case Failure(cause) =>
          state = IoPreparationState.ConsumedFailure(cause)
          startupFailure = startupFailure.orElse(Some(cause))
          future.completeExceptionally(cause)
    catch
      case cause: Throwable =>
        if cause.isInstanceOf[InterruptedException] then Thread.currentThread().interrupt()
        startupFailure = startupFailure.orElse(Some(cause))
        if !consumed then state = IoPreparationState.PendingDeadline(cause)
        // 不覆盖originalcause，不取消future，不释放未消费reservation。
        try io.quiesce()
        catch case cleanup: Throwable => if cleanup ne cause then cause.addSuppressed(cleanup)
        throw cause

object OwnedIoPreparation:
  /** bridge创建的实际control是唯一授权来源；正常Main不能任意指定自身为wait owner。 */
  def forBootstrap[C, A](control: DedicatedBootstrapBridge, io: OwnedSerialIo[C, A],
                         signal: CountDownLatch, tag: IoOperationTag, command: C): OwnedIoPreparation[C, A] =
    require(control != null && control.phase() == DedicatedBootstrapBridge.Phase.PREPARING,
      "actual preparing bootstrap control required")
    val owner = control.controlThread()
    require(owner != null && (Thread.currentThread() eq owner), "only actual bridge control may bind preparation")
    io.requirePreparationControl(owner)
    new OwnedIoPreparation(io, signal, owner, tag, command)
