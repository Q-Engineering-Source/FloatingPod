package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.MySqlRuntimeMetadata
import com.wi.floatingpod.server.io.{IoCompletion, IoOperationTag, IoSubmission}
import java.util.UUID
import java.util.concurrent.TimeUnit

/** Root新受控SQL/native窗口调用；本holder不读取控制/秘密、不发行审批，也不假造原SQL/native资格。 */
private[mysql] final class MySqlCertifiedRecoveryInspectionFixture private (
    val adapter: MySqlOwnedIoAdapter, val tag: IoOperationTag, private val owner: Thread):
  private var completion: IoCompletion[MySqlOwnedIoReply] = null
  private var failure: Throwable = null
  private var submitted = false
  private[mysql] def submit(metadata: MySqlRuntimeMetadata, policy: MySqlRuntimeAuditPolicy,
      request: MySqlCertifiedRestoreRequest): Unit =
    require(Thread.currentThread() eq owner, "fixture initial owner required")
    require(!submitted, "fixture one-shot command required")
    submitted = true
    try
      require(adapter.submit(tag, MySqlOwnedIoCommand.BootCertifiedInspection(metadata, policy, request)) == IoSubmission.Accepted,
        "fixture inspection was not accepted")
    catch
      case cause: Throwable =>
        failure = cause
        throw cause

  /** 非阻塞实际mailbox消费；持有原completion至Root保存与后续责任处理，不转Ready。 */
  def poll(): Option[MySqlCertifiedInspectionObservation] =
    require(Thread.currentThread() eq owner, "fixture original owner consumes the completion")
    if completion == null then
      val batch = adapter.drain(1)
      if batch.nonEmpty then
        completion = batch.head
        require(completion.tag == tag, "actual accepted fixture tag differs")
    if completion == null then None
    else completion.result match
      case scala.util.Success(MySqlOwnedIoReply.CertifiedInspectionObserved(observed)) => Some(observed)
      case scala.util.Failure(cause) =>
        failure = cause
        throw cause
      case _ => throw new IllegalStateException("fixed inspection reply required; original completion retained")

  def retainedCompletion: Option[IoCompletion[MySqlOwnedIoReply]] =
    require(Thread.currentThread() eq owner, "fixture original owner required")
    Option(completion)
  def retainedFailure: Option[Throwable] = Option(failure)
  def quiesce(): Unit =
    require(Thread.currentThread() eq owner, "fixture original owner quiesces")
    adapter.quiesce()
  /** Root调用者必须offMain；actual worker termination不等同native/root责任清理。 */
  def awaitOffMain(limit: Long, unit: TimeUnit): MySqlOwnedIoWorkerTerminationWait =
    adapter.awaitWorkerTerminationOffMain(limit, unit)

private[mysql] object MySqlCertifiedRecoveryInspectionFixture:
  /** 在任何submit/JDBC/native副作用之前返回注册holder。Root随后调用其固定submit方法。 */
  def allocate(actualBoot: UUID, bootGeneration: Long, connectionGeneration: Long): MySqlCertifiedRecoveryInspectionFixture =
    val owner = Thread.currentThread()
    val identity = MySqlOwnedIoIdentity(actualBoot, bootGeneration, connectionGeneration)
    val adapter = new MySqlOwnedIoAdapter(1, owner, "certified-recovery-isolated-fixture", identity)
    new MySqlCertifiedRecoveryInspectionFixture(adapter,
      IoOperationTag(actualBoot, bootGeneration, connectionGeneration, UUID.randomUUID()), owner)
