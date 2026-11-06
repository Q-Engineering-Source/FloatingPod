package com.wi.floatingpod.server

import com.wi.floatingpod.debug.{DebugItemEndpoint, MainPodOperationRequest, PodDebugRuntimeRegistry}
import com.wi.floatingpod.domain.{FloatingPodPose, PodOperatingMode}
import com.wi.floatingpod.server.io.IoCompletion
import com.wi.floatingpod.storage.mysql.MySqlOwnedIoReply
import net.minecraft.entity.player.EntityPlayerMP
import net.minecraft.world.World
import java.util.UUID

/** 仅Main本地会话检查；从不传到worker，不是Ready/SQL能力。
  * production实现重核 actual player connection/world/registry lease；tests只替换游戏适配边界。
  */
private[floatingpod] trait MainControlledPublicationSession:
  def assertCurrent(request: MainPodOperationRequest, expectedWorld: PodWorld): Unit

private[floatingpod] final class ForgeMainControlledPublicationSession(player: EntityPlayerMP, world: World)
    extends MainControlledPublicationSession:
  require(player != null && world != null, "actual publication player/world required")
  private val owner = Thread.currentThread()
  override def assertCurrent(request: MainPodOperationRequest, expectedWorld: PodWorld): Unit =
    if Thread.currentThread() ne owner then throw new IllegalStateException("Publication session requires Main")
    require(request != null && request.action() == DebugItemEndpoint.Action.PLACE && request.playerRequest().identity().pod() == null,
      "original exact placement request required")
    expectedWorld match
      case forge: PodWorldImpl if forge.rawWorld eq world => ()
      case _ => throw new IllegalStateException("Publication physical world differs from original session world")
    if !PodDebugRuntimeRegistry.matchesCurrent(request.playerRequest(), player, world, null) then
      throw new IllegalStateException("Original placement session/world/boot is no longer current")

/** 无实体/票据/session/JDBC引用的immutable inspect值；不是persisted readback。 */
final case class MainControlledPodView(podUuid: UUID, ownerUuid: Option[UUID], operatingMode: PodOperatingMode,
    active: Boolean, stateVersion: Long, pose: FloatingPodPose, phase: DebugPodPhase,
    reason: DebugItemEndpoint.Reason)

private[floatingpod] enum MainControlledPublicationStatus:
  case Retained, Published, AlreadySet, Faulted, Retired

/** 从retained scope产生；failed/stale仍保留exact完成和物理责任，不表示可删除。 */
private[floatingpod] final case class MainControlledPublicationObservation(
    status: MainControlledPublicationStatus, view: Option[MainControlledPodView], cause: Option[Throwable],
    completionConsumed: Boolean)

/** Caller保留这个未匹配完成；异常携exactimmutable值，不把stale完成默默丢弃。 */
private[floatingpod] final class MainPublicationUnmatchedCompletion(
    val completion: com.wi.floatingpod.server.io.IoCompletion[com.wi.floatingpod.storage.mysql.MySqlOwnedIoReply])
    extends IllegalStateException("Completion has no exact retained Main publication operation")

/** Main retained原连接/body资格；不是SQL admission能力。 */
private[floatingpod] trait MainOwnedModePublicationSession:
  def assertCurrent(request: DebugItemEndpoint.Request, expectedWorld: PodWorld): Unit

/** 仅Main内部的未收口责任观察；resource presence不是cleanup/B2/Ready证明。 */
private[server] final case class MainDebugResponsibility(
    request: MainPodOperationRequest, stage: MainDebugStage,
    completions: Vector[IoCompletion[MySqlOwnedIoReply]], cause: Option[Throwable],
    retainsBody: Boolean, retainsTicket: Boolean,
    preparation: Option[com.wi.floatingpod.storage.mysql.MySqlControlledDebugPreparedGraph],
    modeRequest: Option[com.wi.floatingpod.storage.mysql.MySqlOwnedModeCommitRequest],
    plans: Vector[MainDebugPlanReservation],
    dbFirstPreparation:Option[com.wi.floatingpod.storage.mysql.MySqlDbFirstInitialPreparationDataView]=None,
    initialPublication:Option[MainControlledPublicationObservation]=None)
