package com.wi.floatingpod.server

import com.wi.floatingpod.storage.mysql.*
import com.wi.floatingpod.server.io.{IoOperationTag, IoSubmission}
import net.minecraft.entity.player.EntityPlayerMP
import net.minecraft.server.MinecraftServer
import net.minecraft.world.WorldServer
import java.util.UUID
import com.wi.floatingpod.deployment.command.MainDeploymentOriginalOperationIndex
import scala.collection.mutable

/** 标量交付账本；不发行 Session/plan/Ready，stale 原件保持到停止观察。 */
private[server] final class MainRequestReadDelivery(main: Thread, capacity: Int):
  require(main != null && capacity > 0 && capacity <= 256)
  final class Entry(val tag: IoOperationTag, val epoch: Long):
    var decision = Option.empty[MySqlRequestReadDecision]
    var failure = Option.empty[Throwable]
    var consumed = false
  private val entries = mutable.LinkedHashMap.empty[IoOperationTag, Entry]
  private var retired = false
  private def owner(): Unit = require(Thread.currentThread() eq main, "exact Main delivery owner required")
  def begin(tag: IoOperationTag, epoch: Long): Option[Entry] =
    owner(); require(tag != null && epoch >= 0 && !entries.contains(tag))
    if retired || entries.size >= capacity then None
    else
      val value = new Entry(tag, epoch)
      entries.put(tag, value)
      Some(value)
  def fail(value: Entry, cause: Throwable): Unit =
    owner(); require(entries.get(value.tag).exists(_ eq value) && cause != null)
    value.failure match
      case None => value.failure = Some(cause)
      case Some(original) => if (original ne cause) && !original.getSuppressed.contains(cause) then original.addSuppressed(cause)
  def publish(value: Entry, epoch: Long, decision: MySqlRequestReadDecision): Unit =
    owner(); require(entries.get(value.tag).exists(_ eq value) && decision != null)
    if retired || epoch != value.epoch then fail(value, new IllegalStateException("read delivery epoch retired"))
    else if value.failure.isEmpty then
      require(value.decision.isEmpty && !value.consumed, "decision delivered once")
      value.decision = Some(decision)
  /** 同一生产边界接收exact-tag poll，保原cause；正常Pending不伪失败。 */
  def observe(value: Entry, epoch: Long, poll: MainRequestReadPoll): Unit =
    owner(); require(poll != null)
    poll match
      case MainRequestReadPoll.Pending => ()
      case MainRequestReadPoll.Failed(cause) => fail(value, cause)
      case MainRequestReadPoll.Decision(result) => publish(value, epoch, result)
      case MainRequestReadPoll.OriginalDeployment(_) => fail(value,new IllegalStateException("preview delivery received original S-read"))
  def consume(value: Entry, epoch: Long): Option[MySqlRequestReadDecision] =
    owner(); require(entries.get(value.tag).exists(_ eq value))
    if retired || epoch != value.epoch then fail(value, new IllegalStateException("read consumption epoch retired"))
    value.failure.foreach(cause => throw cause)
    if value.consumed then throw new IllegalStateException("read delivery already consumed")
    value.decision.map { result =>
      value.consumed = true
      value.decision = None
      entries.remove(value.tag)
      result
    }
  /** 停止交付仍消费原决定；Pending不伪造结果，失败保留原entry/cause。 */
  def consumeForPreStop(value: Entry, epoch: Long, poll: MainRequestReadPoll): Boolean =
    owner()
    observe(value, epoch, poll)
    consume(value, epoch).nonEmpty
  def requireSettledForPreStop(): Unit =
    require(!pendingForPreStop, "request-read delivery retains original responsibilities")
  def pendingForPreStop: Boolean =
    owner()
    entries.valuesIterator.flatMap(_.failure).take(1).foreach(throw _)
    require(!retired, "request-read delivery retired before drain")
    entries.nonEmpty
  def retire(): Unit = { owner(); retired = true }
  def retainedCount: Int = { owner(); entries.size }

/** 原 claimed runtime 的 Main-only 桥；只有 immutable 三查询进入同一 owned worker。 */
private[floatingpod] final class MainOwnedRequestReadBridge(
    server: MinecraftServer, adapter: MySqlOwnedIoAdapter, capacity: Int = 256):
  require(server != null && adapter != null && server.isCallingFromMinecraftThread)
  private val main = server.getServerThread
  private val identity = adapter.identity
  private val preview = new MainOwnedRequestReadPreview(adapter, main, capacity, Math.min(32, capacity))
  private val delivery = new MainRequestReadDelivery(main, capacity)
  private final class Context(val player: EntityPlayerMP, val world: WorldServer,
      val connection: AnyRef, val request: MySqlRequestReadPreview, val entry: delivery.Entry):
    def current(): Unit =
      require(player.getServer eq server, "original player server changed")
      require((player.connection eq connection) && (player.world eq world) &&
        (server.getPlayerList.getPlayerByUUID(request.playerUuid) eq player), "original player connection/world changed")
  private val contexts = mutable.LinkedHashMap.empty[IoOperationTag, Context]
  private final class OriginalContext(val actor:com.wi.floatingpod.network.MainDeploymentActorContext,
      val index:MainDeploymentOriginalOperationIndex,val entry:MainDeploymentOriginalOperationIndex#Entry,val epoch:Long):
    val operation:UUID=entry.operation()
    var failure=Option.empty[Throwable]
    def current():Unit =
      actor.requireCurrent()
      require(adapter.repositorySubmissionEpoch==epoch,"original deployment S-read epoch retired")
      failure.foreach(throw _)
  private val originalContexts=mutable.LinkedHashMap.empty[IoOperationTag,OriginalContext]
  private var stopped = false
  private var businessAdmissionSealed = false
  private var lastEnd = 0L
  private def owner(): Unit = require((Thread.currentThread() eq main) && server.isCallingFromMinecraftThread)
  private[floatingpod] def sealNewRequestsForPreStop(): Unit =
    owner()
    businessAdmissionSealed = true
  private[floatingpod] def preStopReadDrainRequested: Boolean =
    owner(); businessAdmissionSealed
  private[floatingpod] def preStopReadDrainPending: Boolean =
    owner()
    require(businessAdmissionSealed && !stopped, "original sealed live request-read bridge required")
    val deliveryPending = delivery.pendingForPreStop
    val previewPending = preview.pendingForPreStop
    contexts.nonEmpty || originalContexts.nonEmpty || deliveryPending || previewPending
  /** 认证连接捕获的actor是唯一查询身份；不接收payload中的actor UUID或执行mutation。 */
  def submitOriginal(actor:com.wi.floatingpod.network.MainDeploymentActorContext,
      index:MainDeploymentOriginalOperationIndex,entry:MainDeploymentOriginalOperationIndex#Entry,
      tag:IoOperationTag,workflow:UUID,reserve:UUID):IoSubmission =
    owner()
    if businessAdmissionSealed then return IoSubmission.RejectedStopped(None)
    require(!stopped && actor!=null && index!=null && entry!=null && !originalContexts.contains(tag) && !contexts.contains(tag))
    actor.requireCurrent()
    require(entry.actor()==actor.actorUuid && entry.stage()==MainDeploymentOriginalOperationIndex.Stage.LOOKING_UP &&
      (index.observePending(actor.actorUuid,entry.operation()).orElse(null) eq entry) &&
      !originalContexts.valuesIterator.exists(_.entry eq entry),"exact original authenticated index lookup required")
    if originalContexts.size+contexts.size>=capacity then return IoSubmission.RejectedCapacity
    val context=new OriginalContext(actor,index,entry,adapter.repositorySubmissionEpoch)
    originalContexts.put(tag,context)
    try
      val result=preview.submitOriginal(tag,MySqlRepositoryOperation.FindOriginalDeploymentOperation(actor.actorUuid,context.operation),workflow,reserve)
      if result!=IoSubmission.Accepted then originalContexts.remove(tag)
      result
    catch case cause:Throwable=>
      context.failure=Some(cause)
      try index.retainFailure(entry,cause)
      catch case secondary:Throwable=>if secondary ne cause then cause.addSuppressed(secondary)
      throw cause
  /** 外层None是pending，Some(None)只来自实际reader的Missing；失败保原context。 */
  def consumeOriginal(tag:IoOperationTag):Option[Option[MainDeploymentOriginalOperationIndex.LookupMatch]] =
    owner()
    require(!businessAdmissionSealed,"original S-read delivery sealed for pre-stop")
    consumeOriginalResult(tag,true)
  private def consumeOriginalResult(tag:IoOperationTag,checkActor:Boolean):Option[Option[MainDeploymentOriginalOperationIndex.LookupMatch]] =
    val context=originalContexts.getOrElse(tag,throw new IllegalArgumentException("original S-read tag required"))
    try
      if checkActor then context.current()
      else
        context.failure.foreach(throw _)
        require(adapter.repositorySubmissionEpoch==context.epoch,"original S-read pre-stop epoch retired")
      preview.poll(tag) match
        case MainRequestReadPoll.Pending=>None
        case MainRequestReadPoll.Failed(cause)=>throw cause
        case MainRequestReadPoll.OriginalDeployment(result)=>
          require(result.forall(row=>row.actorUuid==context.actor.actorUuid && row.originalOperationId==context.operation),
            "original S-read consumer identity differs")
          val decision=result match
            case Some(row)=>Some(context.index.found(context.entry,row.actorUuid,row.request))
            case None=>context.index.absent(context.entry);None
          originalContexts.remove(tag)
          Some(decision) // 完整row/request在原调用栈消费；不向外泄漏需root承担的强引用。
        case _=>throw new IllegalStateException("original S-read received preview decision")
    catch case cause:Throwable=>
      context.failure=Some(cause)
      try context.index.retainFailure(context.entry,cause)
      catch case secondary:Throwable=>if secondary ne cause then cause.addSuppressed(secondary)
      throw cause
  /** Root controlled SQL harness and later asynchronous packet route use this exact entry; no network Accepted. */
  def submit(player: EntityPlayerMP, pod: UUID, expectedVersion: Long, tag: IoOperationTag,
      workflow: UUID, reserve: UUID): IoSubmission =
    owner()
    if businessAdmissionSealed then return IoSubmission.RejectedStopped(None)
    require(!stopped && player != null && pod != null)
    if contexts.size+originalContexts.size>=capacity then return IoSubmission.RejectedCapacity
    require(!originalContexts.contains(tag),"tag already retained by original S-read")
    identity.requireTag(tag)
    val world = player.world match
      case actual: WorldServer => actual
      case _ => throw new IllegalArgumentException("actual server world required")
    val request = MySqlRequestReadPreview(pod, player.getUniqueID, expectedVersion)
    val epoch = adapter.repositorySubmissionEpoch
    val allocated = delivery.begin(tag, epoch)
    if allocated.isEmpty then return IoSubmission.RejectedCapacity
    val context = new Context(player, world, player.connection, request, allocated.get)
    contexts.put(tag, context) // 捕获后首次校验/提交失败仍强持原责任。
    try
      context.current()
      val result = preview.submit(tag, request, workflow, reserve)
      if result != IoSubmission.Accepted then delivery.fail(context.entry,
        new IllegalStateException("read submission was not accepted"))
      result
    catch
      case cause: Throwable => delivery.fail(context.entry, cause); throw cause
  def onEnd(tick: PermanentEndTick): Unit =
    owner(); require(tick != null && tick.bootUuid == identity.bootId &&
      tick.bootGeneration == identity.bootGeneration && tick.endOrdinal > lastEnd)
    lastEnd = tick.endOrdinal
    if !stopped then
      preview.onEnd(tick.endOrdinal) // shared admission/checkpoint failures propagate to original runtime boundary.
      if businessAdmissionSealed then
        originalContexts.keysIterator.toVector.foreach(tag=>consumeOriginalResult(tag,false))
        // 快照只含既有有界Context引用；本END不发布给玩家，后续END仍承担原drop/ACK。
        contexts.valuesIterator.toVector.foreach { context =>
          try
            if delivery.consumeForPreStop(context.entry, adapter.repositorySubmissionEpoch,
                preview.poll(context.entry.tag)) then contexts.remove(context.entry.tag)
          catch
            case cause: Throwable =>
              delivery.fail(context.entry, cause)
              throw cause
        }
  def consume(tag: IoOperationTag): Option[MySqlRequestReadDecision] =
    owner()
    require(!businessAdmissionSealed, "request-read delivery sealed for pre-stop")
    val context = contexts.getOrElse(tag, throw new IllegalArgumentException("original request tag required"))
    try
      context.current()
      delivery.observe(context.entry, adapter.repositorySubmissionEpoch, preview.poll(tag))
      val result = delivery.consume(context.entry, adapter.repositorySubmissionEpoch)
      if result.nonEmpty then contexts.remove(tag) // preview 原 plan/root 仍由后续 END exact release 持有。
      result
    catch
      case cause: Throwable =>
        delivery.fail(context.entry, cause)
        throw cause
  /** 仅原read分支的严格检查；不签一般consumer闭合或停止许可。 */
  private[floatingpod] def requirePreStopReadDrainSettled(): Unit =
    require(!preStopReadDrainPending, "sealed request-read contexts must actually drain")
  def stop(): Unit =
    owner(); stopped = true; delivery.retire(); preview.retire() // 不造 drop receipt/worker ACK。
  def retainedCount: Int = { owner(); contexts.size + originalContexts.size + preview.retainedCount }
