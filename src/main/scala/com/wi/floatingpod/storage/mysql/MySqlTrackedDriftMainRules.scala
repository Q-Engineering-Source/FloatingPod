package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.FloatingPodState

/** Pure data validation; only manager's retained exact completion/body scope may publish the result. */
private[floatingpod] object MySqlTrackedDriftMainRules:
  final case class Rebase(durableBase: FloatingPodState, latestCache: FloatingPodState, keepDirty: Boolean)
  def acknowledge(currentCriticalBase: FloatingPodState, latestDesired: FloatingPodState,
      request: MySqlTrackedDriftRequest, durable: FloatingPodState,
      capturedRevision: Long, currentRevision: Long): Rebase =
    require(currentCriticalBase != null && latestDesired != null && request != null && durable != null &&
      capturedRevision > 0L && currentRevision >= capturedRevision, "exact retained drift revision inputs required")
    require(exactState(currentCriticalBase, request.criticalBase),
      "critical preimage advanced while drift was in flight; retained completion cannot overwrite it")
    MySqlTrackedDriftRequest.requireSameChunkPoseOnly(request.binding, currentCriticalBase, latestDesired, request.attemptUuid)
    require(durable.uuid == currentCriticalBase.uuid && durable.ownerUuid == currentCriticalBase.ownerUuid &&
      durable.operatingMode == currentCriticalBase.operatingMode && durable.isActive &&
      durable.worldName == currentCriticalBase.worldName && durable.dimensionId == currentCriticalBase.dimensionId &&
      durable.stateVersion == Math.addExact(currentCriticalBase.stateVersion, 1L) &&
      samePoseBits(durable, request.desired),
      "drift ACK must be actual requested pose and exact critical version +1")
    if currentRevision == capturedRevision then
      require(exactState(latestDesired, request.desired), "captured revision's desired state was replaced without a revision")
      Rebase(durable, durable, false)
    else
      Rebase(durable, latestDesired.copy(stateVersion = durable.stateVersion, lastUpdated = durable.lastUpdated), true)

  private[floatingpod] def samePoseBits(a: FloatingPodState, b: FloatingPodState): Boolean =
    java.lang.Double.doubleToRawLongBits(a.posX) == java.lang.Double.doubleToRawLongBits(b.posX) &&
      java.lang.Double.doubleToRawLongBits(a.posY) == java.lang.Double.doubleToRawLongBits(b.posY) &&
      java.lang.Double.doubleToRawLongBits(a.posZ) == java.lang.Double.doubleToRawLongBits(b.posZ) &&
      java.lang.Float.floatToRawIntBits(a.yaw) == java.lang.Float.floatToRawIntBits(b.yaw) &&
      java.lang.Float.floatToRawIntBits(a.pitch) == java.lang.Float.floatToRawIntBits(b.pitch)
  private def exactState(a: FloatingPodState, b: FloatingPodState): Boolean =
    a.uuid == b.uuid && a.ownerUuid == b.ownerUuid && a.stateVersion == b.stateVersion &&
      a.worldName == b.worldName && a.isActive == b.isActive && a.lastUpdated == b.lastUpdated &&
      a.dimensionId == b.dimensionId && a.operatingMode == b.operatingMode && samePoseBits(a, b)
