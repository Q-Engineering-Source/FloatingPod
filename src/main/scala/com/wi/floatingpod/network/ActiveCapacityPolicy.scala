package com.wi.floatingpod.network

import com.wi.floatingpod.storage.RecoveryPodStore

/** Release 容量判定的窄边界，保留每次请求读取数据库活动意图快照的责任。 */
trait ReleaseCapacityPolicy:
  def canRelease: Boolean

/** 以数据库原始 desired-active 行计算当前 Release 是否仍有可用容量的 policy。 */
final class ActiveCapacityPolicy(store: RecoveryPodStore, configuredLimit: Int) extends ReleaseCapacityPolicy:
  require(store != null, "store must not be null")
  require(configuredLimit >= 1 && configuredLimit <= 256, "configuredLimit must be between 1 and 256")

  /** 回传本次判定使用的活动意图数量，不读取 runtime entity 映射或构造公开状态。 */
  def activeCount: Int = store.findDesiredActiveRows().get.size

  /** 只有活动数量严格小于注入限制时，后续 Release 才能进行。 */
  def canRelease: Boolean = activeCount < configuredLimit
