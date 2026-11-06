package com.wi.floatingpod.server.io

import java.util.UUID
import java.time.Duration
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

final case class IoOperationTag(bootId: UUID, bootGeneration: Long, connectionGeneration: Long, operationId: UUID):
  require(bootId != null && operationId != null, "exact boot and operation identities required")
  require(bootGeneration > 0L && connectionGeneration > 0L, "IO generations must be positive")
final case class IoCompletion[A](tag: IoOperationTag, result: Try[A])

/** opaque receipt；只有 pending receipt 的 reference identity 能交接消费责任。 */
final class IoOwnerTransfer private (
    private[io] val instance: AnyRef, private[io] val oldOwner: Thread,
    private[io] val newOwner: Thread, val bootTag: IoOperationTag):
  override def toString: String = "IoOwnerTransfer[opaque]"

object IoOwnerTransfer:
  private[io] def issue(instance: AnyRef, oldOwner: Thread, newOwner: Thread, bootTag: IoOperationTag): IoOwnerTransfer =
    new IoOwnerTransfer(instance, oldOwner, newOwner, bootTag)

enum IoSubmission:
  case Accepted
  case RejectedCapacity
  case RejectedQuiescing
  case RejectedStopped(cause: Option[Throwable])

/** callback 观察，不声明 SQL close、ACK、durability 或 IO exit proof。 */
enum IoWorkerExitHookObservation:
  case NoHook
  case HookReturned
  case HookFailed(cause: Throwable)

/** 仅实际 OwnedSerialIo worker 的 bounded join 结果；不代表 JDBC、DB fence 或 server barrier 终止。 */
enum IoWorkerTerminationWait:
  case Terminated(proof: IoWorkerTerminationProof)
  case TimedOut
  case Interrupted(cause: InterruptedException)

/** opaque proof；其签发者、具体 IO 实例与实际 worker thread 均由 issuer reference 校验。 */
final class IoWorkerTerminationProof private[io] (
    private[io] val owner: AnyRef,
    private[io] val worker: Thread,
    private[io] val issuer: AnyRef):
  override def toString: String = "IoWorkerTerminationProof[opaque]"

private[io] object IoWorkerTerminationProof:
  def issue(owner: AnyRef, worker: Thread, issuer: AnyRef): IoWorkerTerminationProof =
    require(owner != null && worker != null && issuer != null, "exact worker termination issuer required")
    new IoWorkerTerminationProof(owner, worker, issuer)

/** 框架装配的 owned worker maintenance；不属于 Main 可提交的 command。
  * 返回下一次正值有界间隔；异常是 worker fatal，不能吞成健康观察。
  */
trait IoWorkerMaintenance:
  def maintain(): IoMaintenanceDeadline

/** 只描述框架 timer；origin 是计算 delay 时的实际 monotonic 观察，不在返回后重新锚定。 */
final case class IoMaintenanceDeadline(observedAtNanos: Long, delay: Duration):
  require(delay != null && !delay.isNegative && !delay.isZero && delay.compareTo(Duration.ofMinutes(30)) <= 0,
    "maintenance interval must be positive and at most 30 minutes")
  private[floatingpod] val delayNanos: Long = delay.toNanos
  private[floatingpod] def remainingAt(now: Long): Long =
    val elapsed = now - observedAtNanos
    require(elapsed >= 0L, "maintenance monotonic origin is in the future")
    delayNanos - elapsed

object IoMaintenanceDeadline:
  def after(delay: Duration): IoMaintenanceDeadline = IoMaintenanceDeadline(System.nanoTime(), delay)

final case class IoCheckpoint(accepting: Boolean, queued: Int, running: Boolean, completions: Int,
                              reserved: Int, stopped: Boolean, fatalCause: Option[Throwable],
                              workerExitHook: Option[IoWorkerExitHookObservation] = None,
                              exitFailure: Option[Throwable] = None,
                              maintenanceScheduled: Boolean = false,
                              maintenanceRunning: Boolean = false)

/** 单一非 daemon worker；调用方负责只提交不可变值，并在 Main 复核完整 tag 后发布。
  * drain 不过滤旧代际：已执行副作用的退休完成仍须由 owner 明确收口。
  * capacity 同时限制等待执行与未消费完成；接受时预留完成槽，禁止取消丢失责任。
  */
final class OwnedSerialIo[C, A](capacity: Int, mainThread: Thread, workerName: String,
                               workerExitHook: Option[() => Unit] = None,
                               completionSignal: Option[CountDownLatch] = None,
                               workerMaintenance: Option[IoWorkerMaintenance] = None)(execute: C => A):
  require(capacity > 0, "capacity must be positive")
  require(mainThread != null, "mainThread required")
  require(execute != null, "owned IO execution function required")
  require(workerExitHook != null && !workerExitHook.exists(_ == null), "exit hook option/function required")
  require(completionSignal != null && !completionSignal.exists(_ == null), "completion signal option required")
  require(workerMaintenance != null && !workerMaintenance.exists(_ == null), "maintenance option required")
  private var currentCompletionSignal=completionSignal
  private[floatingpod] def ownsCompletionSignal(signal: CountDownLatch): Boolean =
    lock.synchronized { currentCompletionSignal.exists(_ eq signal) }
  /** 仅原control在上一真实completion已消费后轮换；不能重置信号或跳过mailbox责任。 */
  private[floatingpod] def rotatePreparationCompletionSignal(previous:CountDownLatch):CountDownLatch = lock.synchronized {
    requirePreparationControl(Thread.currentThread())
    require(currentCompletionSignal.exists(_ eq previous) && previous.getCount==0L,
      "original consumed preparation signal required")
    require(accepting && !stopped && fatalCause.isEmpty && !running && !maintenanceRunning &&
      reserved==0 && pending.isEmpty && completed.isEmpty,
      "preparation signal rotation requires healthy idle IO and consumed mailbox")
    val next=new CountDownLatch(1)
    currentCompletionSignal=Some(next)
    next
  }
  private[floatingpod] def requirePreparationControl(control: Thread): Unit = lock.synchronized {
    require((control eq mainThread) && (control ne worker) && currentOwner.exists(_ eq control),
      "preparation requires exact initial current control owner, never worker or transferred Main")
  }
  /** 专用Boot独占尚未消费的mailbox；原submit容量/owner/停止规则仍在同lock执行。 */
  private[floatingpod] def submitPreparation(tag: IoOperationTag, command: C): IoSubmission = lock.synchronized {
    requirePreparationControl(Thread.currentThread())
    if reserved != 0 then IoSubmission.RejectedCapacity
    else submit(tag, command)
  }
  private def signalCompletion(): Unit = currentCompletionSignal.foreach(_.countDown())
  private val lock = new Object
  private val pending = mutable.Queue.empty[(IoOperationTag, C)]
  private val completed = mutable.Queue.empty[IoCompletion[A]]
  private val exited = new CountDownLatch(1)
  private val workerTerminationIssuer = new Object
  private var accepting = true
  private var running = false
  private var stopped = false
  private var reserved = 0
  private var fatalCause = Option.empty[Throwable]
  private var exitHookObservation = Option.empty[IoWorkerExitHookObservation]
  private var exitFailure = Option.empty[Throwable]
  private var currentOwner = Option(mainThread)
  private var transferIssued = false
  private var transferTarget = Option.empty[Thread]
  private var pendingTransfer = Option.empty[IoOwnerTransfer]
  private var transferAbortCause = Option.empty[Throwable]
  private var maintenanceDeadline = Option.empty[IoMaintenanceDeadline]
  private var maintenanceRunning = false
  private val worker = new Thread(() => runWorker(), workerName)
  worker.setDaemon(false)
  worker.start()

  /** 初始 Disabled。只能由该实际 worker 在成功初始化后安排，不授予 Main 等待/SQL 能力。 */
  private[floatingpod] def scheduleMaintenanceAfter(delay: Duration): Unit = lock.synchronized {
    require(Thread.currentThread() eq worker, "maintenance scheduling requires exact owned worker")
    scheduleMaintenanceDeadline(IoMaintenanceDeadline.after(delay))
  }

  private[floatingpod] def scheduleMaintenanceDeadline(value: IoMaintenanceDeadline): Unit = lock.synchronized {
    require(Thread.currentThread() eq worker, "maintenance scheduling requires exact owned worker")
    require(workerMaintenance.nonEmpty, "maintenance was not installed")
    require(value != null, "actual maintenance deadline required")
    value.remainingAt(System.nanoTime()) // 允许已 due，但禁止未来 origin；不重新加时。
    if accepting && !stopped && fatalCause.isEmpty then
      maintenanceDeadline = Some(value)
      lock.notifyAll()
  }

  private def requireMain(): Unit =
    require(currentOwner.exists(_ eq Thread.currentThread()), "operation requires current owner Main thread")

  /** 一次性移交；调用方在取得 receipt 后才启动这个真实 NEW thread。无 DB/Ready/exit 含义。 */
  def issueOwnerTransfer(target: Thread, bootTag: IoOperationTag): IoOwnerTransfer = lock.synchronized {
    requireMain()
    require(!transferIssued, "owner transfer may only be issued once")
    require(accepting && !stopped && fatalCause.isEmpty, "owner transfer requires healthy accepting worker")
    require(target != null && (target ne mainThread) && (target ne worker), "transfer requires distinct target Main thread")
    require(target.getState == Thread.State.NEW, "target Main thread must be NEW before receipt issuance")
    require(bootTag != null, "exact boot tag required")
    val receipt = IoOwnerTransfer.issue(this, mainThread, target, bootTag)
    transferIssued = true
    transferTarget = Some(target)
    pendingTransfer = Some(receipt)
    currentOwner = None
    receipt
  }

  /** fatal/stop 不撤销消费责任；claim 仅恢复 owner，不复活 worker 或 accepting。 */
  def claimOwnerTransfer(receipt: IoOwnerTransfer): Unit = lock.synchronized {
    require(receipt != null && (receipt.instance eq this), "transfer receipt belongs to another IO instance")
    require(pendingTransfer.exists(_ eq receipt), "transfer receipt is stale, replayed or not issued")
    require(Thread.currentThread() eq receipt.newOwner, "only exact target Main may claim receipt")
    require(receipt.oldOwner eq mainThread, "transfer original owner mismatch")
    currentOwner = Some(receipt.newOwner)
    pendingTransfer = None
  }

  /** 仅收回 shutdown 消费责任；原启动/claim 失败独立可观察，不伪装 worker fatal。
    * 校验与失权/停止接受在同一 lock；已接受 work、完成与 reservation 保持原样。
    * old owner 只能在 target 未启动或已终止时收回，不能抢夺正在运行的 target。
    */
  def abortOwnerTransfer(receipt: IoOwnerTransfer, originalCause: Throwable): Unit = lock.synchronized {
    require(receipt != null && (receipt.instance eq this), "transfer receipt belongs to another IO instance")
    require(originalCause != null, "original transfer failure cause required")
    require(pendingTransfer.exists(_ eq receipt), "transfer receipt is stale, replayed or already claimed")
    require((receipt.oldOwner eq mainThread) && transferTarget.exists(_ eq receipt.newOwner), "exact transfer owners required")
    val caller = Thread.currentThread()
    require((caller eq receipt.newOwner) ||
      ((caller eq receipt.oldOwner) &&
        (receipt.newOwner.getState == Thread.State.NEW || receipt.newOwner.getState == Thread.State.TERMINATED)),
      "abort requires exact preclaim target or old owner with NEW/TERMINATED target")
    pendingTransfer = None
    accepting = false
    maintenanceDeadline = None
    transferAbortCause = Some(originalCause)
    currentOwner = Some(receipt.oldOwner)
    lock.notifyAll()
  }

  /** 原 cause 保留 reference identity；不是 DB、worker clean 或 IO exit 证明。 */
  def ownerTransferAbortCause: Option[Throwable] = lock.synchronized { transferAbortCause }

  def submit(tag: IoOperationTag, command: C): IoSubmission =
    lock.synchronized {
      requireMain()
      require(tag != null, "exact operation tag required before reserving work")
      if stopped || fatalCause.nonEmpty then IoSubmission.RejectedStopped(fatalCause.orElse(exitFailure))
      else if !accepting then IoSubmission.RejectedQuiescing
      else if reserved == capacity then IoSubmission.RejectedCapacity
      else
        pending.enqueue((tag, command))
        reserved += 1
        lock.notifyAll()
        IoSubmission.Accepted
    }

  def drain(max: Int): Vector[IoCompletion[A]] =
    lock.synchronized {
      requireMain()
      require(max >= 0, "max must be nonnegative")
      val result = Vector.newBuilder[IoCompletion[A]]
      var remaining = max
      while remaining > 0 && completed.nonEmpty do
        result += completed.dequeue()
        reserved -= 1
        remaining -= 1
      result.result()
    }

  /** 不等待 I/O；已接受 work 仍依序执行。 */
  def quiesce(): Unit =
    lock.synchronized { requireMain(); accepting = false; maintenanceDeadline = None; lock.notifyAll() }

  /** 只在本实例真实worker退出hook调用；无观察对象，也不消费队列或制造终结许可。 */
  private[floatingpod] def requireTerminalHookEmpty(): Unit = lock.synchronized {
    if (Thread.currentThread() ne worker) || accepting || pending.nonEmpty || completed.nonEmpty ||
        reserved != 0 || running || maintenanceRunning || maintenanceDeadline.nonEmpty then
      throw new IllegalStateException("actual worker tail still retains accepted IO/completion/maintenance responsibility")
    fatalCause match
      case Some(cause) => throw cause
      case None => ()
  }

  def checkpoint: IoCheckpoint = lock.synchronized {
    IoCheckpoint(accepting, pending.size, running, completed.size, reserved, stopped, fatalCause, exitHookObservation, exitFailure,
      maintenanceDeadline.nonEmpty, maintenanceRunning)
  }

  /** 唯一允许等待的退出尾端入口。超时返回 false，不能解读为关闭成功。 */
  def awaitExitOffMain(timeout: Long, unit: TimeUnit): Boolean =
    lock.synchronized {
      require(!(Thread.currentThread() eq mainThread) && !transferTarget.exists(_ eq Thread.currentThread()), "exit wait forbidden on initial or target Main")
      require(!(Thread.currentThread() eq worker), "worker cannot await its own exit")
      require(timeout >= 0, "timeout must be nonnegative")
    }
    exited.await(timeout, unit)
  /**
    * 等待 exact worker thread 有界 join 后再签发 IO-only 退役证明。
    * exited latch 先于 runWorker 真正退栈；它和 checkpoint/exit hook 不足以证明 thread TERMINATED。
    * initial/target Main 与 worker 本身禁止等待；timeout、interrupt 与仍存活线程均不给证明。
    */
  def awaitWorkerTerminationOffMain(timeout: Long, unit: TimeUnit): IoWorkerTerminationWait =
    val caller = Thread.currentThread()
    val timeoutNanos = lock.synchronized {
      require(!(caller eq mainThread) && !transferTarget.exists(_ eq caller), "worker termination wait forbidden on initial or target Main")
      require(!(caller eq worker), "worker cannot await its own termination")
      require(timeout >= 0, "timeout must be nonnegative")
      require(unit != null, "worker termination timeout unit required")
      unit.toNanos(timeout)
    }
    def terminated: Boolean = worker.getState == Thread.State.TERMINATED && !worker.isAlive
    def proof(): IoWorkerTerminationWait =
      require(terminated, "worker termination proof requires the exact thread to be TERMINATED")
      IoWorkerTerminationWait.Terminated(IoWorkerTerminationProof.issue(this, worker, workerTerminationIssuer))

    if caller.isInterrupted then
      IoWorkerTerminationWait.Interrupted(new InterruptedException("worker termination wait began with caller already interrupted"))
    else
      try
        val joined = worker.join(Duration.ofNanos(timeoutNanos))
        if caller.isInterrupted then
          IoWorkerTerminationWait.Interrupted(new InterruptedException("worker termination wait caller was interrupted"))
        else if joined && terminated then proof()
        else IoWorkerTerminationWait.TimedOut
      catch
        case cause: InterruptedException =>
          caller.interrupt()
          IoWorkerTerminationWait.Interrupted(cause)

  /** Adapter 只能验证从自己的实际 IO worker 返回的 opaque proof。 */
  private[floatingpod] def requireWorkerTerminationProof(proof: IoWorkerTerminationProof): Unit =
    require(proof != null && (proof.owner eq this) && (proof.worker eq worker) &&
      (proof.issuer eq workerTerminationIssuer), "worker termination proof belongs to another IO instance/generation")
    require(worker.getState == Thread.State.TERMINATED && !worker.isAlive,
      "worker termination proof no longer matches the actual worker state")

  private def runWorker(): Unit =
    try
      if workerMaintenance.nonEmpty then runMaintainedWorker()
      else runOriginalWorker()
    catch
      case cause: Throwable =>
        lock.synchronized { stopFatally(cause); signalCompletion() } // 原锁内绑定fatal与当前信号。
    finally
      lock.synchronized { accepting = false; running = false; maintenanceDeadline = None; maintenanceRunning = false }
      finishWorkerExit()

  /** None maintenance 的既有执行循环保持原样。 */
  private def runOriginalWorker(): Unit =
      var continue = true
      while continue do
        val next = lock.synchronized {
          while pending.isEmpty && accepting do lock.wait()
          if pending.nonEmpty then
            running = true
            Some(pending.dequeue())
          else None
        }
        next match
          case None => continue = false
          case Some((tag, command)) =>
            val result = try Success(execute(command)) catch
              case NonFatal(cause) => Failure(cause)
              case cause: Throwable =>
                lock.synchronized {
                  completed.enqueue(IoCompletion(tag, Failure(cause)))
                  stopFatally(cause)
                  signalCompletion()
                }
                throw cause
            lock.synchronized {
              completed.enqueue(IoCompletion(tag, result))
              running = false
              signalCompletion()
            }

  private def runMaintainedWorker(): Unit =
    var continue = true
    while continue do
      val next = lock.synchronized {
        var selected = Option.empty[Either[Unit, (IoOperationTag, C)]]
        var selecting = true
        while selecting do
          val now = System.nanoTime()
          val remaining = maintenanceDeadline.map(_.remainingAt(now))
          if accepting && remaining.exists(_ <= 0L) then
            maintenanceDeadline = None
            maintenanceRunning = true
            selected = Some(Left(()))
            selecting = false
          else if pending.nonEmpty then
            running = true
            selected = Some(Right(pending.dequeue()))
            selecting = false
          else if !accepting then selecting = false
          else remaining match
            case Some(nanos) => lock.wait(nanos / 1000000L, (nanos % 1000000L).toInt)
            case None => lock.wait()
        selected
      }
      next match
        case None => continue = false
        case Some(Left(_)) =>
          // 已在 lock 选中的义务即使随后 quiesce 仍执行一次；不得另线程 close 或撤销。
          val startedAt = System.nanoTime()
          val deadline = workerMaintenance.get.maintain()
          require(deadline != null && deadline.observedAtNanos - startedAt >= 0L,
            "recurring maintenance must return its actual current invocation origin")
          deadline.remainingAt(System.nanoTime())
          require(deadline.delayNanos >= 1000000L, "recurring maintenance interval below 1ms would hot-loop; deadline is not extended")
          lock.synchronized {
            maintenanceRunning = false
            if accepting && fatalCause.isEmpty then maintenanceDeadline = Some(deadline)
          }
        case Some(Right((tag, command))) =>
          val result = try Success(execute(command)) catch
            case NonFatal(cause) => Failure(cause)
            case cause: Throwable =>
              lock.synchronized {
                completed.enqueue(IoCompletion(tag, Failure(cause)))
                stopFatally(cause)
                signalCompletion()
              }
              throw cause
          lock.synchronized {
            completed.enqueue(IoCompletion(tag, result))
            running = false
            signalCompletion()
          }

  private def finishWorkerExit(): Unit =
      // 原实际 worker，锁外执行一次；callback 卡住时只能观察 wait timeout，不能另线程偷 close。
      val observation = workerExitHook match
        case None => IoWorkerExitHookObservation.NoHook
        case Some(hook) =>
          try
            hook()
            IoWorkerExitHookObservation.HookReturned
          catch case cause: Throwable => IoWorkerExitHookObservation.HookFailed(cause)
      lock.synchronized {
        observation match
          case IoWorkerExitHookObservation.HookFailed(cleanupCause) =>
            fatalCause match
              case Some(primary) =>
                if (primary ne cleanupCause) && !primary.getSuppressed.contains(cleanupCause) then
                  primary.addSuppressed(cleanupCause)
                exitFailure = Some(primary)
              case None => exitFailure = Some(cleanupCause)
          case _ => exitFailure = fatalCause
        exitHookObservation = Some(observation)
        stopped = true
      }
      // 终态已记录才发布退出尾端信号；这不是 callback 所涉及资源的独立 close 证明。
      exited.countDown()

  private def stopFatally(cause: Throwable): Unit =
    accepting = false
    running = false
    maintenanceDeadline = None
    maintenanceRunning = false
    if fatalCause.isEmpty then fatalCause = Some(cause)
    // 未执行 work 以同一原始原因显式失败，保留每个 operation 的完成责任。
    while pending.nonEmpty do
      val (tag, _) = pending.dequeue()
      completed.enqueue(IoCompletion(tag, Failure(cause)))
