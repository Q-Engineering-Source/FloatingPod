package com.wi.floatingpod.domain

import java.util.UUID

/**
 * 表示玩家在持久化权威边界上的注册与同时活动额度。
 *
 * @param playerUuid 已认证玩家的稳定标识。
 * @param registrationLimit 可注册辅助机的最大数量。
 * @param simultaneousActiveLimit 可同时活动辅助机的最大数量。
 */
final case class PlayerEntitlement(
    playerUuid: UUID,
    registrationLimit: Int = PlayerEntitlement.DefaultRegistrationLimit,
    simultaneousActiveLimit: Int = PlayerEntitlement.DefaultSimultaneousActiveLimit
):
  require(playerUuid != null, "playerUuid must not be null")
  require(registrationLimit >= 0, "registrationLimit must not be negative")
  require(simultaneousActiveLimit >= 0, "simultaneousActiveLimit must not be negative")

object PlayerEntitlement:
  /** 未配置外部权益时，数据库懒创建的规范默认注册额度。 */
  val DefaultRegistrationLimit: Int = 1

  /** 未配置外部权益时，数据库懒创建的规范默认同时活动额度。 */
  val DefaultSimultaneousActiveLimit: Int = 1
