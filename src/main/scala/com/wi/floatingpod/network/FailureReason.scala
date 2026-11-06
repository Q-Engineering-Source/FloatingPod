package com.wi.floatingpod.network

/** 以稳定小整数表示可安全回传客户端的失败原因。 */
sealed trait FailureReason:
  /** 封包中的固定原因代码，不承载自由格式文字。 */
  def wireId: Int

/** Release 请求可回报的有限失败集合。 */
enum ReleaseFailureReason(val wireId: Int) extends FailureReason:
  case NotOwner extends ReleaseFailureReason(1)
  case VersionMismatch extends ReleaseFailureReason(2)
  case AlreadyActive extends ReleaseFailureReason(3)
  case CapacityReached extends ReleaseFailureReason(4)
  case InvalidPosition extends ReleaseFailureReason(5)
  case TicketFailed extends ReleaseFailureReason(6)
  case UnknownPod extends ReleaseFailureReason(7)
  case EntitlementDenied extends ReleaseFailureReason(8)
  case InfrastructureFailure extends ReleaseFailureReason(9)
  /** 本次请求未接受；服务端正在交接当前操作，不代表本请求已进入队列。 */
  case Busy extends ReleaseFailureReason(10)

object ReleaseFailureReason:
  /** 从 wire id 还原有限枚举，未知值立即拒绝。 */
  def fromWireId(wireId: Int): ReleaseFailureReason =
    values.find(_.wireId == wireId).getOrElse(throw new IllegalArgumentException(s"Unknown release failure reason $wireId"))

/** Recall 请求可回报的有限失败集合。 */
enum RecallFailureReason(val wireId: Int) extends FailureReason:
  case NotOwner extends RecallFailureReason(1)
  case VersionMismatch extends RecallFailureReason(2)
  case NotActive extends RecallFailureReason(3)
  case WrongDimension extends RecallFailureReason(4)
  case UnknownPod extends RecallFailureReason(5)
  case InfrastructureFailure extends RecallFailureReason(6)
  /** 本次请求未接受；不得据此更换或重发已有UNKNOWN operation。 */
  case Busy extends RecallFailureReason(7)

object RecallFailureReason:
  /** 从 wire id 还原有限枚举，未知值立即拒绝。 */
  def fromWireId(wireId: Int): RecallFailureReason =
    values.find(_.wireId == wireId).getOrElse(throw new IllegalArgumentException(s"Unknown recall failure reason $wireId"))
