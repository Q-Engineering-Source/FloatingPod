package com.wi.floatingpod.server

import com.wi.floatingpod.bootstrap.ProductionDbFirstBootstrapCallbacks
import com.wi.floatingpod.domain.RecallRequestNamespace
import com.wi.floatingpod.storage.mysql.{MySqlDbFirstRecallRequestLookup,MySqlRequestReadDecision,MySqlRequestReadPreview}
import com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission
import java.util.UUID
import net.minecraft.entity.player.EntityPlayerMP

private[floatingpod] object MainDbFirstReadAdmission:
  final class Sealed private[MainDbFirstReadAdmission] () extends IllegalStateException("Original schema6 request-read admission/delivery sealed")

/** 实际Main词法读责任；没有worker Work，因此不伪造提交、drop或ACK。 */
private[floatingpod] final class MainDbFirstReadAdmission(main:Thread):
  require(main!=null,"actual Main required")
  private var sealedAdmission=false
  private var active=0
  private var failure=Option.empty[Throwable]
  private def owner():Unit = require(Thread.currentThread() eq main,"exact schema6 request-read Main required")
  def withRead[A](read: => A):A =
    owner();failure.foreach(throw _)
    if sealedAdmission then throw new MainDbFirstReadAdmission.Sealed()
    active=Math.incrementExact(active)
    try
      val result=try read catch
        // 原successor暂拒与封门拒绝没有DATA失败语义；仍不向调用者交付部分结果。
        case expected:ManagedOrdinarySaveAdmission.MainReadPending=>throw expected
        case expected:MainDbFirstReadAdmission.Sealed=>throw expected
        case cause:Throwable=>
          failure match
            case None=>failure=Some(cause)
            case Some(original)=>if (original ne cause) && !original.getSuppressed.contains(cause) then original.addSuppressed(cause)
          throw cause
      failure.foreach(throw _)
      if sealedAdmission then throw new MainDbFirstReadAdmission.Sealed()
      result
    finally active-=1
  def sealNewRequestsForPreStop():Unit =
    owner();sealedAdmission=true
  /** 只观察词法借用是否退出；false不表示读成功或DATA健康，不准据此启动新producer。 */
  def preStopReadScopesPending:Boolean =
    owner()
    require(sealedAdmission,"original schema6 read admission must actually be sealed")
    active!=0
  def preStopReadFailureAfterScopesEnded:Option[Throwable] =
    require(!preStopReadScopesPending,"original schema6 lexical read scopes remain active")
    // 非fatal原读失败继续保管，给既有operation独立核验/收ACK的机会；Fatal不延后。
    failure.foreach { cause=>if !scala.util.control.NonFatal(cause) then throw cause }
    failure
  def preStopReadDrainPending:Boolean =
    owner();failure.foreach(throw _)
    require(sealedAdmission,"original schema6 read admission must actually be sealed")
    active!=0
  def requirePreStopReadDrainSettled():Unit =
    require(!preStopReadDrainPending,"original schema6 lexical reads remain active")

/** 认证玩家的同步标量预览；只借原callback当前Full27，不转交旧Session worker命令。 */
private[floatingpod] final class DbFirstMainRequestReadBridge(callbacks:ProductionDbFirstBootstrapCallbacks):
  require(callbacks!=null,"original schema6 callback required")
  private val server=callbacks.actualServer
  private val main=server.getServerThread
  private val admission=new MainDbFirstReadAdmission(main)
  private def owner():Unit =
    require((Thread.currentThread() eq main) && server.isCallingFromMinecraftThread,"actual schema6 server Main required")
    callbacks.requireOriginalMainAdapter(callbacks.adapter)
  private def authenticatedRead[A](player:EntityPlayerMP)(read:UUID=>A):A =
    owner()
    require(player!=null && (player.getServer eq server) && player.connection!=null &&
      (server.getPlayerList.getPlayerByUUID(player.getUniqueID) eq player),"actual authenticated player required")
    val actor=player.getUniqueID
    require(actor!=null && actor!=new UUID(0L,0L),"nonzero authenticated actor required")
    val connection=player.connection
    admission.withRead {
      val result=read(actor)
      require(player.getUniqueID==actor && (player.connection eq connection) && (player.getServer eq server) &&
        (server.getPlayerList.getPlayerByUUID(player.getUniqueID) eq player),"original read actor connection changed")
      result
    }
  def preview(player:EntityPlayerMP,pod:UUID,expectedVersion:Long):MySqlRequestReadDecision =
    require(pod!=null && expectedVersion>=0L,"valid schema6 preview target/version required")
    authenticatedRead(player) { actor=>
      val data=callbacks.recoveryReadData // 一次借用同一代；完整DATA不逃逸词法作用域。
      MySqlRequestReadPreview.decideCurrent(pod,actor,expectedVersion,
        data.state(pod),data.entitlement(actor),data.activeCountFor(actor))
    }
  def recallNamespace(player:EntityPlayerMP):RecallRequestNamespace =
    authenticatedRead(player)(callbacks.recallRequestNamespace)
  /** 查询只交付当前代的独立metadata；未命中不允许新建或重执行operation。 */
  def lookupRecallClientRequest(player:EntityPlayerMP,requested:RecallRequestNamespace,
      request:UUID):MySqlDbFirstRecallRequestLookup =
    require(requested!=null && request!=null && request!=new UUID(0L,0L),"valid recall namespace and client request required")
    authenticatedRead(player)(actor=>callbacks.lookupRecallClientRequest(actor,requested,request))
  def lookupRecallOriginalOperation(player:EntityPlayerMP,requested:RecallRequestNamespace,
      operation:UUID):MySqlDbFirstRecallRequestLookup =
    require(requested!=null && operation!=null && operation!=new UUID(0L,0L),"valid recall namespace and original operation required")
    authenticatedRead(player)(actor=>callbacks.lookupRecallOriginalOperation(actor,requested,operation))
  private[floatingpod] def sealNewRequestsForPreStop():Unit =
    owner();admission.sealNewRequestsForPreStop()
  private[floatingpod] def preStopReadDrainPending:Boolean =
    owner();admission.preStopReadDrainPending
  private[floatingpod] def preStopReadScopesPending:Boolean =
    owner();admission.preStopReadScopesPending
  private[floatingpod] def preStopReadFailureAfterScopesEnded:Option[Throwable] =
    owner();admission.preStopReadFailureAfterScopesEnded
  private[floatingpod] def requirePreStopReadDrainSettled():Unit =
    owner();admission.requirePreStopReadDrainSettled()
