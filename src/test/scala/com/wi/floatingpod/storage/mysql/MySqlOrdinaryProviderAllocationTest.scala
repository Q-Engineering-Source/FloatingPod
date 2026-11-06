package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** 实际原root与独立Main线程；不创建custody，不取得SQL/游戏资格。 */
final class MySqlOrdinaryProviderAllocationTest:
  private def withMain(body:(MySqlFormat3ReadBudget,MySqlDbFirstSourceAllocation)=>Unit):Unit =
    val profile=SourceLimitProfile.of(1048576L,1048576L,1048576L,64L,256L,8388608L,1048576L,1048576L)
    val root=new MySqlFormat3ReadBudget(profile,profile.maxSqlPacketBytes())
    val allocation=new MySqlDbFirstSourceAllocation(root,Thread.currentThread())
    val failed=new AtomicReference[Throwable]()
    val main=new Thread(()=> {
      try body(root,allocation)
      catch case cause:Throwable=>failed.set(cause)
    },"ordinary-provider-allocation-main")
    allocation.bindMain(main)
    main.start();main.join(3000L)
    assertFalse(main.isAlive,"Main allocation test timed out")
    if failed.get()!=null then throw failed.get()
    root.closeRootReadBudget()

  @Test def nestedScopesRefundOnlyTheirOriginalReservations():Unit = withMain { (root,allocation)=>
    val before=root.availableMemoryBytes
    val scope=allocation.retainOrdinaryProvider(256L,"actual provider scope")
    val snapshot=allocation.retainOrdinaryProvider(1024L,"actual provider snapshot")
    assertEquals(before-1280L,root.availableMemoryBytes)
    snapshot.close()
    assertEquals(before-256L,root.availableMemoryBytes)
    assertThrows(classOf[IllegalArgumentException],()=>snapshot.close())
    assertEquals(before-256L,root.availableMemoryBytes)
    scope.close()
    assertEquals(before,root.availableMemoryBytes)
  }

  @Test def failedReservationRetainsCauseAndDoesNotEraseExistingCharge():Unit = withMain { (root,allocation)=>
    val before=root.availableMemoryBytes
    val scope=allocation.retainOrdinaryProvider(256L,"actual active scope")
    val original=assertThrows(classOf[java.io.IOException],()=>
      allocation.retainOrdinaryProvider(before,"actual over-capacity snapshot"))
    assertInstanceOf(classOf[java.sql.SQLException],original.getCause)
    assertEquals(before-256L,root.availableMemoryBytes)
    val retained=assertThrows(classOf[java.io.IOException],()=>
      allocation.retainOrdinaryProvider(1L,"later reservation after failure"))
    assertSame(original.getCause,retained.getCause)
    scope.close()
    assertEquals(before,root.availableMemoryBytes)
  }

  @Test def foreignThreadCannotRefundMainCharge():Unit = withMain { (root,allocation)=>
    val before=root.availableMemoryBytes
    val scope=allocation.retainOrdinaryProvider(256L,"actual Main scope")
    val refused=new AtomicReference[Throwable]()
    val foreign=new Thread(()=> {
      try scope.close()
      catch case cause:Throwable=>refused.set(cause)
    },"foreign-provider-close")
    foreign.start();foreign.join(3000L)
    assertFalse(foreign.isAlive,"foreign close timed out")
    assertInstanceOf(classOf[IllegalArgumentException],refused.get())
    assertEquals(before-256L,root.availableMemoryBytes)
    scope.close()
    assertEquals(before,root.availableMemoryBytes)
  }
