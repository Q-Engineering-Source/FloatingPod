package com.wi.floatingpod.storage.mysql

import java.util.UUID
import com.wi.floatingpod.domain.PlayerEntitlement

/** Main认证入口捕获的固定只读参数；UUID本身不发行认证或mutation权限。 */
final case class MySqlRequestReadPreview(podUuid: UUID, playerUuid: UUID, expectedVersion: Long):
  require(podUuid != null && playerUuid != null && expectedVersion >= 0L, "fixed read-preview identity required")
  val operations: Vector[MySqlRepositoryOperation] = Vector(
    MySqlRepositoryOperation.FindByUuid(podUuid),
    MySqlRepositoryOperation.FindByPlayerUuid(playerUuid),
    MySqlRepositoryOperation.FindDesiredActiveRows)

/** 只包含标量观察，不持有snapshot、state、read预算、数据库能力或Ready。 */
enum MySqlRequestOwnershipPreview:
  case Missing, NotOwner
  case VersionMismatch(actual: Long)
  case Matched(version: Long, active: Boolean)

final case class MySqlRequestReadDecision(ownership: MySqlRequestOwnershipPreview,
    activeOwned: Int, simultaneousLimit: Int):
  require(ownership != null && activeOwned >= 0 && simultaneousLimit >= 0, "valid preview scalar result required")
  def belowEntitlementLimit: Boolean = activeOwned < simultaneousLimit

object MySqlRequestReadPreview:
  /** 同一次full20投影返回的固定三个reply；不接受额外结果或任意dispatcher。 */
  private[floatingpod] def decide(request: MySqlRequestReadPreview, replies: Vector[MySqlRepositoryReply]): MySqlRequestReadDecision =
    require(request != null && replies != null, "original request and exact replies required")
    replies match
      case Vector(MySqlRepositoryReply.OptionalState(state), MySqlRepositoryReply.Entitlement(entitlement),
          MySqlRepositoryReply.StoredRows(rows)) =>
        require(state != null && entitlement != null && rows != null && state.forall(_ != null) && entitlement.forall(_ != null) && rows.forall(_ != null),
          "NULL carrier is not SQL NULL")
        state.foreach(value => require(value.uuid == request.podUuid, "foreign pod read result"))
        entitlement.foreach(value => require(value.playerUuid == request.playerUuid, "foreign player read result"))
        // 借用原immutable List逐项验证，不在Main再复制全列表/构建unreserved Set。
        var remaining = rows
        while remaining.nonEmpty do
          val current = remaining.head
          require(current.isActive && !remaining.tail.exists(_.uuid == current.uuid),
            "desired-active vector must preserve unique actual active rows")
          remaining = remaining.tail
        decideCurrent(request.podUuid,request.playerUuid,request.expectedVersion,state,entitlement,
          rows.count(_.ownerUuid.contains(request.playerUuid)))
      case _ => throw new IllegalStateException("fixed three-reply read-preview protocol mismatch")
  /** 原已确认Main DATA的同一代标量预览；不发行认证、SQL提交或Ready资格。 */
  private[floatingpod] def decideCurrent(pod:UUID,player:UUID,expectedVersion:Long,
      state:Option[com.wi.floatingpod.domain.FloatingPodState],entitlement:Option[PlayerEntitlement],
      activeOwned:Int):MySqlRequestReadDecision =
    require(pod!=null && player!=null && expectedVersion>=0L && state!=null && entitlement!=null && activeOwned>=0,
      "exact current Main request-read inputs required")
    state.foreach(value=>require(value!=null && value.uuid==pod,"foreign pod Main read result"))
    entitlement.foreach(value=>require(value!=null && value.playerUuid==player,"foreign player Main read result"))
    val ownership=state match
      case None=>MySqlRequestOwnershipPreview.Missing
      case Some(value) if !value.ownerUuid.contains(player)=>MySqlRequestOwnershipPreview.NotOwner
      case Some(value) if value.stateVersion!=expectedVersion=>MySqlRequestOwnershipPreview.VersionMismatch(value.stateVersion)
      case Some(value)=>MySqlRequestOwnershipPreview.Matched(value.stateVersion,value.isActive)
    val limit=entitlement.map(_.simultaneousActiveLimit).getOrElse(PlayerEntitlement.DefaultSimultaneousActiveLimit)
    MySqlRequestReadDecision(ownership,activeOwned,limit)
