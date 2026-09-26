package com.wi.floatingpod.storage

import com.wi.floatingpod.domain.PlayerEntitlement

import java.util.UUID
import scala.util.Try

/**
 * 为 Release 预览读取玩家已持久化的权益，不负责创建或修改权益记录。
 */
trait PlayerEntitlementStore:
  /**
   * 按已认证玩家 UUID 查询明确持久化的权益；不存在时返回 `None`。
   */
  def findByPlayerUuid(playerUuid: UUID): Try[Option[PlayerEntitlement]]
