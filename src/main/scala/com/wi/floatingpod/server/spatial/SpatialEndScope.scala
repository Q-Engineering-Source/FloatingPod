package com.wi.floatingpod.server.spatial

import com.wi.floatingpod.server.{PermanentEndTick, PermanentMainPumpCore, PermanentPumpLease}

/** 仅在原 END 的同步消费栈内有效；保留对象或 tick 都不能延长资格。 */
private[floatingpod] final class SpatialEndScope private (
    permit: PermanentMainPumpCore.SpatialScopePermit,
    tick: PermanentEndTick
):
  def consume[A](consumer: PermanentEndTick => A): A =
    permit.validate()
    require(consumer != null, "spatial scope consumer is required")
    try consumer(tick)
    catch
      case cause: Throwable =>
        permit.failed(cause)
        throw cause

  /** 原consumer绑定其真实lease；相同boot值不能替代原core签发的引用身份。 */
  def consumeFor[A](originalLease: PermanentPumpLease)(consumer: PermanentEndTick => A): A =
    permit.validateExactLease(originalLease)
    require(consumer != null, "spatial scope consumer is required")
    try consumer(tick)
    catch
      case cause: Throwable =>
        permit.failed(cause)
        throw cause

private[floatingpod] object SpatialEndScope:
  /** permit 只由原 core 创建且不向消费者暴露；本工厂不签发资格。 */
  private[server] def create(
      permit: PermanentMainPumpCore.SpatialScopePermit,
      tick: PermanentEndTick
  ): SpatialEndScope =
    require(permit != null && tick != null, "original core permit and END are required")
    new SpatialEndScope(permit, tick)
