package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.DbFirstBootActiveRecovery

/** Session独立支付Boot metadata；普通Main DATA读代退休不得退款，且不保留RAW投影。 */
private[mysql] final class MySqlDbFirstBootRecallCensus private (
    private var root:MySqlFormat3ReadBudget,private val worker:Thread,
    private var records:Vector[MySqlDbFirstRecallRequestRecord],private var fee:Long):
  private var claimant:DbFirstBootActiveRecovery=null
  private var main:Thread=null
  private var claimed=false
  private var detached=false
  private var retired=false
  private var retirementAttempted=false
  private final class Takeover(var intent:MySqlDbFirstBootRecallTakeoverIntent):
    val originalTag=intent.original.originalTag
    var detached=false
    var started=false
    var reservation:MySqlDbFirstCaptureReservation=null
    var mainClaimed=false
  private var takeovers=Vector.empty[Takeover]
  def beginTakeover(owner:DbFirstBootActiveRecovery,record:MySqlDbFirstRecallRequestRecord,
      control:com.wi.floatingpod.server.io.IoOperationTag,execution:com.wi.floatingpod.server.io.IoOperationTag):MySqlDbFirstBootRecallTakeoverIntent = synchronized {
    require(!retired && claimed && !detached && (owner eq claimant) && (Thread.currentThread() eq main) &&
      record!=null && records.exists(_ eq record) && !takeovers.exists(_.originalTag==record.originalTag),
      "same actual Boot census original may be claimed once")
    owner.requireIssuingBootTakeover(record,control,execution)
    val next=Math.addExact(fee,8192L)
    root.reserveRetainedMemory(8192L,"Boot intent/registry/control/refusal END metadata BEFORE allocation")
    fee=next
    val intent=new MySqlDbFirstBootRecallTakeoverIntent(control,execution,record)
    takeovers=takeovers:+new Takeover(intent)
    intent
  }
  def beginTakeoverOnWorker(intent:MySqlDbFirstBootRecallTakeoverIntent):Unit = synchronized {
    require((Thread.currentThread() eq worker) && !retired && claimed && !detached,"actual live Boot census worker required")
    val entry=takeovers.find(_.intent eq intent).getOrElse(throw new IllegalArgumentException("unregistered Boot takeover intent"))
    require(!entry.started,"original Boot control worker attempt is one-shot")
    entry.started=true
  }
  def completeTakeoverOnWorker(intent:MySqlDbFirstBootRecallTakeoverIntent,reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    require((Thread.currentThread() eq worker) && !retired && !detached,"actual Boot census completion worker required")
    val entry=takeovers.find(_.intent eq intent).getOrElse(throw new IllegalArgumentException("original Boot intent missing"))
    require(entry.started && entry.reservation==null && reservation!=null && reservation.tag==intent.executionTag,
      "original Boot inactive C completion may be recorded once")
    entry.reservation=reservation
  }
  def claimTakeoverOnMain(owner:DbFirstBootActiveRecovery,intent:MySqlDbFirstBootRecallTakeoverIntent,
      reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    require(!retired && claimed && !detached && (owner eq claimant) && (Thread.currentThread() eq main),
      "actual original Boot completion Main required")
    val entry=takeovers.find(_.intent eq intent).getOrElse(throw new IllegalArgumentException("original Boot intent missing"))
    require(!entry.mainClaimed && entry.reservation!=null && (entry.reservation eq reservation),
      "exact original inactive C result must be claimed once")
    entry.mainClaimed=true
  }
  def requireMainClaimedTakeover(owner:DbFirstBootActiveRecovery,intent:MySqlDbFirstBootRecallTakeoverIntent,
      reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    require(!retired && claimed && !detached && (owner eq claimant) && (Thread.currentThread() eq main),
      "actual original Boot Main decision owner required")
    val entry=takeovers.find(_.intent eq intent).getOrElse(throw new IllegalArgumentException("unregistered Boot decision intent"))
    require(entry.started && entry.mainClaimed && (entry.reservation eq reservation),
      "actual claimed inactive C must precede Main decision")
  }
  def detachTakeover(owner:DbFirstBootActiveRecovery,intent:MySqlDbFirstBootRecallTakeoverIntent,
      reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    require(!retired && claimed && !detached && (owner eq claimant) && (Thread.currentThread() eq main),
      "actual original Boot takeover detach Main required")
    val entry=takeovers.find(_.intent eq intent).getOrElse(throw new IllegalArgumentException("original Boot detach intent missing"))
    require(entry.started && entry.mainClaimed && !entry.detached && (entry.reservation eq reservation),
      "same registered actual Boot reservation must detach once")
    owner.requireDetachingBootTakeover(intent,reservation)
    entry.intent=null;entry.reservation=null;entry.detached=true
  }
  def claim(owner:DbFirstBootActiveRecovery):Vector[MySqlDbFirstRecallRequestRecord] = synchronized {
    require(!retired && !claimed && owner!=null && (Thread.currentThread() ne worker),
      "original Boot census Main claim is one-shot")
    claimant=owner;main=Thread.currentThread();claimed=true
    records
  }
  def detach(owner:DbFirstBootActiveRecovery):Unit = synchronized {
    require(!retired && claimed && !detached && owner!=null && (owner eq claimant) &&
      (Thread.currentThread() eq main),"same actual Boot census claimant must detach on original Main")
    owner.requireBootCensusDetachedForRetention()
    // 实际Boot owner已撤custody/pass；先撤本holder的对象边，worker后续才能退费。
    records=null;takeovers=Vector.empty;claimant=null;main=null;detached=true
  }
  private[mysql] def requireClaimedOwner(owner:DbFirstBootActiveRecovery):Unit = synchronized {
    require(!retired && claimed && !detached && owner!=null && (owner eq claimant) &&
      (Thread.currentThread() eq main),"same actual live Boot census Main claimant required")
  }
  /** 停止分支不推定terminal结果；未解worker/native图仍阻止撤claim和退款。 */
  private[mysql] def detachForStoppedMain(owner:DbFirstBootActiveRecovery):Unit = synchronized {
    requireClaimedOwner(owner)
    owner.requireStoppedBootCensusDetachedForRetention()
    require(takeovers.forall(t=>t.detached && t.intent==null && t.reservation==null),
      "actual original Boot takeover references remain unsettled at Main stop")
    records=null;takeovers=Vector.empty;claimant=null;main=null;detached=true
  }
  /** 转管保留同一claimant/record/fee；仅原C Main责任已撤边后移交，绝不提前退款。 */
  private[mysql] def requireWorkerTransfer(actualRoot:MySqlFormat3ReadBudget,actualWorker:Thread):Unit = synchronized {
    require((Thread.currentThread() eq worker) && (actualWorker eq worker) && (actualRoot eq root) &&
      !retired && !retirementAttempted && claimed && !detached && claimant!=null && main!=null &&
      records!=null && fee>0L && takeovers.forall(t=>t.detached && t.intent==null && t.reservation==null),
      "actual live claimed Boot census with drained old takeover graph required")
  }
  def retireOnWorker():Unit = synchronized {
    require((Thread.currentThread() eq worker) && !retired && !retirementAttempted && (!claimed || detached),
      "Boot census Main references must end before worker retirement")
    retirementAttempted=true
    records=null
    root.releaseRetainedMemory(fee,"independent Boot recall metadata references detached")
    fee=0L;root=null;retired=true
  }

private[mysql] object MySqlDbFirstBootRecallCensus:
  // 独立重付完整record/vector、custody/pass/最终END图及逐请求结果枚举/Option map；
  // 额外256B支付一次initial terminal登记期，历史terminal无须借J takeover费用。
  // 不假定旧DATA费用能跨代存活。
  def retainedBytes(count:Int):Long =
    require(count>=0,"complete Boot record count required")
    Math.addExact(3584L,Math.multiplyExact(count.toLong,2576L))
  def retain(root:MySqlFormat3ReadBudget,worker:Thread,
      records:Vector[MySqlDbFirstRecallRequestRecord]):MySqlDbFirstBootRecallCensus =
    require(root!=null && !root.isClosed && !root.isScopedReadBudget && worker!=null &&
      (Thread.currentThread() eq worker) && records!=null && records.forall(_!=null),
      "actual worker Session root and complete metadata census required")
    val fee=retainedBytes(records.size)
    root.reserveRetainedMemory(fee,"independent Boot recall census BEFORE retention")
    try new MySqlDbFirstBootRecallCensus(root,worker,records,fee)
    catch case cause:Throwable=>
      try root.releaseRetainedMemory(fee,"unpublished Boot census holder did not escape")
      catch case cleanup:Throwable=>if cleanup ne cause then cause.addSuppressed(cleanup)
      throw cause
