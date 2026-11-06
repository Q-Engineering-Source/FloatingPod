package com.wi.floatingpod.server

import com.wi.floatingpod.domain.RecallRequestNamespace
import com.wi.floatingpod.network.{DurableRecallPhase,RecallFailureReason,S2CDurableRecallStatus}
import com.wi.floatingpod.server.io.IoOperationTag
import java.util.UUID

/** 接纳前预留，实际final ACK后只保标量结果；不持Pending、player、RAW或worker receipt。
  * 仅实际客户端接收ACK释放槽，send返回、时钟和持久phase均不释放。
  */
private[server] final class MainRecallResultCustody(main:Thread,capacity:Int):
  require(main!=null && capacity>0 && capacity<=256,"actual bounded Main result custody required")
  final class Slot private[MainRecallResultCustody] (val namespace:RecallRequestNamespace,val request:UUID,
      val pod:UUID,val version:Long,val tag:IoOperationTag):
    private[MainRecallResultCustody] var phase:DurableRecallPhase=null
    private[MainRecallResultCustody] var reason=Option.empty[RecallFailureReason]
    def status:S2CDurableRecallStatus =
      owner();require(live(this) && phase!=null,"actual final ACK result required")
      new S2CDurableRecallStatus(namespace,request,pod,version,Some(tag.operationId),phase,reason)
  private val slots=new Array[Slot](capacity)
  private def owner():Unit=require(Thread.currentThread() eq main,"original result Main required")
  private def live(slot:Slot):Boolean=slot!=null && slots.exists(_ eq slot)
  def completedCount:Int =
    owner();slots.count(slot=>slot!=null && slot.phase!=null)
  def available:Boolean =
    owner();slots.exists(_==null)
  def reserve(namespace:RecallRequestNamespace,request:UUID,pod:UUID,version:Long,tag:IoOperationTag):Slot =
    owner()
    require(namespace!=null && request!=null && request!=new UUID(0L,0L) && pod!=null &&
      pod!=new UUID(0L,0L) && version>=0L && tag!=null,"complete original result identity required")
    require(!slots.exists(slot=>slot!=null && slot.namespace==namespace &&
      (slot.request==request || slot.tag.operationId==tag.operationId)),"duplicate result reservation")
    val index=slots.indexWhere(_==null);require(index>=0,"result capacity must precede journal admission")
    val slot=new Slot(namespace,request,pod,version,tag)
    slots(index)=slot;slot
  def committed(slot:Slot):Unit =
    owner();require(live(slot) && slot.phase==null && slot.version<Long.MaxValue,"one actual committed final ACK required")
    slot.phase=DurableRecallPhase.Committed
  def refused(slot:Slot,reason:RecallFailureReason):Unit =
    owner();require(live(slot) && slot.phase==null && reason!=null,"one actual refusal final ACK required")
    slot.reason=Some(reason);slot.phase=DurableRecallPhase.Refused
  def client(namespace:RecallRequestNamespace,request:UUID):Option[Slot] =
    owner();slots.find(slot=>slot!=null && slot.phase!=null && slot.namespace==namespace && slot.request==request)
  def original(namespace:RecallRequestNamespace,operation:UUID):Option[Slot] =
    owner();slots.find(slot=>slot!=null && slot.phase!=null && slot.namespace==namespace && slot.tag.operationId==operation)
  /** true只说明此metadata槽不再占用；不签发业务/absence/终局权限。重复ACK可确认已释放。 */
  def received(status:S2CDurableRecallStatus):Boolean =
    owner();status.validate()
    require(status.phase==DurableRecallPhase.Committed || status.phase==DurableRecallPhase.Refused,
      "accepted terminal result receipt required")
    val index=slots.indexWhere(slot=>slot!=null && slot.namespace==status.namespace &&
      (slot.request==status.requestUuid || status.operationUuid.contains(slot.tag.operationId)))
    if index<0 then true
    else
      val slot=slots(index)
      if slot.phase==null || slot.request!=status.requestUuid || slot.pod!=status.podUuid ||
          slot.version!=status.expectedStateVersion || !status.operationUuid.contains(slot.tag.operationId) ||
          slot.phase!=status.phase || slot.reason!=status.reason then false
      else
        slots(index)=null;true
