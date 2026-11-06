package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.server.source.SourceForestAllocationBudget
import java.io.IOException
import java.sql.SQLException

/** 原Session账本的source分配责任；Main/IO子账户仍由Root及实际custody认证，不发行业务资格。 */
private[mysql] final class MySqlDbFirstSourceAllocation(
    private var originalRoot:MySqlFormat3ReadBudget,private val worker:Thread)
    extends SourceForestAllocationBudget.Root:
  require(originalRoot!=null && worker!=null && (Thread.currentThread() eq worker) && !originalRoot.isClosed,
    "original worker and live schema6 source allocation root required")
  private val profile=originalRoot.profile
  @volatile private var main:Thread=null
  private var retired=false
  private var held=0L
  private var failure:Throwable=null
  override def exactProfile():SourceLimitProfile=profile
  private[mysql] def bindMain(actual:Thread):Unit = synchronized {
    requireLive()
    require(actual!=null && (actual ne worker) &&
      ((Thread.currentThread() eq worker) || (Thread.currentThread() eq actual)) &&
      (main==null || (main eq actual)),"original schema6 source Main binding required")
    main=actual
  }
  override protected def requireLive():Unit = synchronized {
    require(!retired && originalRoot!=null && !originalRoot.isClosed,"schema6 source allocation is retired")
  }
  override protected def requireMain():Unit = synchronized {
    requireLive()
    require(main!=null && (Thread.currentThread() eq main),"exact claimed schema6 source Main required")
  }
  override protected def reserveRoot(bytes:Long,context:String):Unit = synchronized {
    requireLive()
    if failure!=null then throw new IOException("schema6 source allocation has an unresolved failure",failure)
    require(bytes>=0L,"nonnegative source reservation required")
    val next=Math.addExact(held,bytes)
    try
      originalRoot.reserveTemporaryMemory(bytes,context)
      held=next
    catch case cause:Throwable=>
      retainFailure(cause)
      cause match
        case sql:SQLException=>throw new IOException("schema6 original source reservation failed",sql)
        case other=>throw other
  }
  override protected def releaseRoot(bytes:Long,context:String):Unit = synchronized {
    requireLive()
    require(bytes>=0L && bytes<=held,"source refund exceeds original schema6 responsibility")
    try
      originalRoot.releaseTemporaryMemory(bytes,context)
      held-=bytes
    catch case cause:Throwable=>
      retainFailure(cause)
      cause match
        case sql:SQLException=>throw new IOException("schema6 original source refund failed",sql)
        case other=>throw other
  }
  /** 普通 provider 的词法费用直接归原 Session；不创建 forensic custody/account。 */
  private[mysql] def retainOrdinaryProvider(bytes:Long,purpose:String):AutoCloseable = synchronized {
    requireMain()
    require(bytes>0L && purpose!=null && purpose.nonEmpty,"positive ordinary provider reservation required")
    reserveRoot(bytes,purpose)
    try new AutoCloseable:
      private var closed=false
      override def close():Unit = MySqlDbFirstSourceAllocation.this.synchronized {
        requireMain()
        require(!closed,"ordinary provider charge already released")
        releaseRoot(bytes,purpose)
        closed=true
      }
    catch case cause:Throwable=>
      try releaseRoot(bytes,purpose)
      catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      throw cause
  }
  private def retainFailure(cause:Throwable):Unit =
    if failure==null then failure=cause
    else if failure ne cause then MySqlOwnedSessionLifecycle.suppress(failure,cause)
  /** 原worker只能退休实际零债且无不确定失败的bridge；drop读DATA不会清除source账户。 */
  private[mysql] def retireOnWorker():Unit = synchronized {
    require(Thread.currentThread() eq worker,"original schema6 source retirement worker required")
    requireLive()
    if failure!=null then throw new IllegalStateException("schema6 source allocation settlement failed",failure)
    require(held==0L,"schema6 source accounts remain unsettled")
    retired=true;main=null;originalRoot=null
  }
