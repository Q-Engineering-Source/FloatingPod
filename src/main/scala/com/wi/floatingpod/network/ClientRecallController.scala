package com.wi.floatingpod.network

import java.util.UUID
import scala.collection.mutable

/** 客户端请求关联及用户确认的结果后2秒缓冲；不授予任何服务端mutation资格。 */
final class ClientRecallController(clock: () => Long, send: C2SRecallOperationRequest => Unit):
  require(clock != null && send != null, "actual monotonic clock and request sender required")
  import ClientRecallController.*
  private final class Entry(val request: UUID, val pod: UUID, val version: Long):
    var operation = Option.empty[UUID]
    var phase = Option.empty[RecallOperationPhase]
    var reason = Option.empty[RecallFailureReason]
    var completedAt = Option.empty[Long]
    def snapshot: View = View(request, pod, version, operation, phase, reason, completedAt.nonEmpty)
    def buffered(now: Long): Boolean = completedAt.forall(at => now - at < ResultBufferNanos)
  private var connection: AnyRef = null
  private val entries = mutable.LinkedHashMap.empty[UUID, Entry]
  private def bind(actual: AnyRef): Unit =
    require(actual != null, "actual client connection required")
    if connection ne actual then
      entries.clear(); connection = actual
  def disconnect(): Unit =
    entries.clear(); connection = null
  def bindConnection(actual: AnyRef): Unit = bind(actual)
  def view(pod: UUID): Option[View] = entries.get(pod).map(_.snapshot)

  /** 整个pending/UNKNOWN期间及结果后2秒均不mint请求、不调用sender。 */
  def submit(actual: AnyRef, pod: UUID, expectedVersion: Long): Boolean =
    bind(actual)
    require(pod != null && expectedVersion >= 0L, "valid recall target/version required")
    val now = clock()
    if entries.get(pod).exists(_.buffered(now)) then return false
    entries.filterInPlace((_, entry) => entry.buffered(now))
    if entries.size >= 256 then return false
    val entry = new Entry(UUID.randomUUID(), pod, expectedVersion)
    entries.put(pod, entry) // sender抛错不能推断未发送；继续保留原请求，不自动重发。
    send(new C2SRecallOperationRequest(entry.request, pod, expectedVersion))
    true

  /** 只应用当前连接、原request/pod/version与不变的server operation；旧结果不解锁新请求。 */
  def receive(actual: AnyRef, status: S2CRecallOperationStatus): Option[View] =
    bind(actual); require(status != null, "actual recall status required"); status.validate()
    entries.get(status.podUuid).filter(e => e.request == status.requestUuid && e.version == status.expectedStateVersion).flatMap { entry =>
      if entry.completedAt.nonEmpty then None // 重复/乱序结果不延长结果后缓冲，也不回退terminal。
      else
        require(entry.operation.isEmpty || entry.operation == status.operationUuid, "original recall operation changed")
        require(!entry.phase.contains(RecallOperationPhase.RetainedFailure) || status.phase != RecallOperationPhase.Processing,
          "retained failure cannot become processing without reconciliation")
        entry.operation = status.operationUuid; entry.phase = Some(status.phase); entry.reason = status.reason
        if status.phase == RecallOperationPhase.Committed || status.phase == RecallOperationPhase.Refused then
          entry.completedAt = Some(clock())
        Some(entry.snapshot)
    }

object ClientRecallController:
  final val ResultBufferNanos: Long = 2000000000L
  final case class View(requestUuid: UUID, podUuid: UUID, expectedVersion: Long, operationUuid: Option[UUID],
      phase: Option[RecallOperationPhase], reason: Option[RecallFailureReason], completed: Boolean)
