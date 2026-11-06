package com.wi.floatingpod.domain

import java.lang.{Double as JavaDouble, Float as JavaFloat}

/** 玩家请求关键 mode 转换时由服务端当前实体采集的实际位置与朝向。 */
final case class FloatingPodPose(
    worldName: String,
    posX: Double,
    posY: Double,
    posZ: Double,
    yaw: Float,
    pitch: Float,
    dimensionId: Int
):
  require(worldName != null && worldName.trim.nonEmpty, "worldName must not be blank")
  require(JavaDouble.isFinite(posX), "posX must be finite")
  require(JavaDouble.isFinite(posY), "posY must be finite")
  require(JavaDouble.isFinite(posZ), "posZ must be finite")
  require(JavaFloat.isFinite(yaw), "yaw must be finite")
  require(JavaFloat.isFinite(pitch), "pitch must be finite")
