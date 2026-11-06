package com.wi.floatingpod.network

import com.wi.floatingpod.domain.RecallRequestNamespace
import java.util.UUID
import scala.collection.mutable

/** 原namespace/request跨断线保留；重连不自动重发业务，所有fresh发送先等实际namespace发布。 */
final class DurableClientRecallController(clock:()=>Long,sendNamespace:C2SRecallNamespaceRequest=>Unit,
    sendFresh:C2SDurableRecallRequest=>Unit,sendResume:C2SRecallResumeQuery=>Unit,
    sendReceived:C2SRecallResultReceived=>Unit):
  require(clock!=null && sendNamespace!=null && sendFresh!=null && sendResume!=null && sendReceived!=null,"actual recall client dependencies required")
  import DurableClientRecallController.*
  private final class Entry(val namespace:RecallRequestNamespace,val request:UUID,val pod:UUID,val version:Long):
    var operation=Option.empty[UUID]
    var phase=Option.empty[DurableRecallPhase]
    var reason=Option.empty[RecallFailureReason]
    var completedAt=Option.empty[Long]
    def buffered(now:Long):Boolean=completedAt.forall(at=>now-at<ClientRecallController.ResultBufferNanos)
    def snapshot:View=View(namespace,request,pod,version,operation,phase,reason,completedAt.nonEmpty)
  private val entries=mutable.LinkedHashMap.empty[(RecallRequestNamespace,UUID),Entry]
  // 未确认的接收回执独立保留；两秒只控制新点击，不证明服务端已收到ACK。
  private val receipts=mutable.LinkedHashMap.empty[(RecallRequestNamespace,UUID),View]
  private var connection:AnyRef=null
  private var published=Option.empty[RecallRequestNamespace]
  def bindConnection(actual:AnyRef):Unit =
    require(actual!=null,"actual client connection required")
    if connection ne actual then
      connection=actual;published=None // 不清原UNKNOWN或其他namespace的请求。
  def disconnect(actual:AnyRef):Unit =
    require(actual!=null,"actual disconnect connection required")
    if connection eq actual then
      connection=null;published=None
  def requestNamespace(actual:AnyRef):Unit =
    require(actual!=null && (connection eq actual),"current connection required before namespace query")
    sendNamespace(new C2SRecallNamespaceRequest())
  def receiveNamespace(actual:AnyRef,message:S2CRecallNamespace):Boolean =
    require(message!=null,"actual namespace publication required");message.validate()
    if actual==null || (connection ne actual) then return false
    require(published.isEmpty || published.contains(message.namespace),"namespace changed on original client connection")
    published=Some(message.namespace)
    receipts.valuesIterator.filter(_.namespace==message.namespace).toVector.foreach(acknowledge)
    true
  def currentNamespace:Option[RecallRequestNamespace]=published
  def view(namespace:RecallRequestNamespace,pod:UUID):Option[View]=entries.get((namespace,pod)).map(_.snapshot)
  def currentView(pod:UUID):Option[View]=published.flatMap(view(_,pod))

  def submit(actual:AnyRef,pod:UUID,expectedVersion:Long):Boolean =
    require(actual!=null && (connection eq actual),"current connection required for durable recall")
    DurableRecallCodec.requireId(pod);require(expectedVersion>=0L,"non-negative recall version required")
    if published.isEmpty then return false
    val namespace=published.get
    val now=clock()
    if entries.get((namespace,pod)).exists(_.buffered(now)) then return false
    entries.filterInPlace((_,entry)=>entry.buffered(now))
    val entryKeys=entries.valuesIterator.map(entry=>(entry.namespace,entry.request)).toSet
    if entryKeys.size+receipts.keysIterator.count(key=> !entryKeys.contains(key))>=256 then return false
    val entry=new Entry(namespace,UUID.randomUUID(),pod,expectedVersion)
    entries.put((namespace,pod),entry)
    sendFresh(new C2SDurableRecallRequest(namespace,entry.request,pod,expectedVersion))
    true // sender抛错时原Entry已保留，不能推断未发送或重铸request。

  def receive(actual:AnyRef,status:S2CDurableRecallStatus):Option[View] =
    require(status!=null,"actual durable recall status required");status.validate()
    if actual==null || (connection ne actual) || !published.contains(status.namespace) then return None
    entries.get((status.namespace,status.podUuid)).filter(entry=>
      entry.request==status.requestUuid && entry.version==status.expectedStateVersion).flatMap { entry=>
      if entry.completedAt.nonEmpty then None
      else
        require(entry.operation.isEmpty || entry.operation==status.operationUuid,"original durable C changed")
        require(!entry.phase.contains(DurableRecallPhase.RetainedFailure) || status.phase!=DurableRecallPhase.Processing,
          "retained failure cannot become processing without reconciliation")
        entry.operation=status.operationUuid;entry.phase=Some(status.phase);entry.reason=status.reason
        if status.phase==DurableRecallPhase.Committed || status.phase==DurableRecallPhase.Refused ||
          status.phase==DurableRecallPhase.NotAccepted then entry.completedAt=Some(clock())
        val observed=entry.snapshot
        if entry.completedAt.nonEmpty && status.operationUuid.nonEmpty then
          receipts.put((observed.namespace,observed.requestUuid),observed) // send前保管，抛错/断线均可重发接收ACK。
          acknowledge(observed)
        Some(observed)
    }.orElse {
      receipts.get((status.namespace,status.requestUuid)).filter(matches(_,status)).foreach(acknowledge)
      None
    }

  private def matches(view:View,status:S2CDurableRecallStatus):Boolean =
    view.namespace==status.namespace && view.requestUuid==status.requestUuid && view.podUuid==status.podUuid &&
      view.expectedStateVersion==status.expectedStateVersion && view.operationUuid==status.operationUuid &&
      view.phase.contains(status.phase) && view.reason==status.reason
  private def acknowledge(view:View):Unit =
    sendReceived(new C2SRecallResultReceived(new S2CDurableRecallStatus(view.namespace,view.requestUuid,view.podUuid,
      view.expectedStateVersion,view.operationUuid,view.phase.get,view.reason)))
  /** 只撤接收ACK待确认metadata，不改业务状态或两秒时钟。 */
  def receiveReleased(actual:AnyRef,message:S2CRecallResultReleased):Boolean =
    require(message!=null,"actual result custody release required");message.validate()
    val status=message.result
    if actual==null || (connection ne actual) || !published.contains(status.namespace) then false
    else receipts.get((status.namespace,status.requestUuid)).filter(matches(_,status)) match
      case None=>false
      case Some(_)=>receipts.remove((status.namespace,status.requestUuid));true

  /** 显式原UUID查询只发query，无Entry、fresh request/J/C或自动业务重发。 */
  def resumeOriginal(actual:AnyRef,operation:UUID):Boolean =
    require(actual!=null && (connection eq actual),"current connection required for resume query")
    DurableRecallCodec.requireId(operation)
    if published.isEmpty then false
    else
      sendResume(new C2SRecallResumeQuery(published.get,operation));true
  /** 定位结果只供观察；不改原operation、phase、完成时刻或缓冲。 */
  def receiveResume(actual:AnyRef,result:S2CRecallResumeResult):Option[RecallResumeLookup] =
    require(result!=null,"actual original resume lookup required");result.validate()
    if actual==null || (connection ne actual) || !published.contains(result.namespace) then None
    else Some(result.lookup)

object DurableClientRecallController:
  final case class View(namespace:RecallRequestNamespace,requestUuid:UUID,podUuid:UUID,expectedStateVersion:Long,
      operationUuid:Option[UUID],phase:Option[DurableRecallPhase],reason:Option[RecallFailureReason],completed:Boolean)
