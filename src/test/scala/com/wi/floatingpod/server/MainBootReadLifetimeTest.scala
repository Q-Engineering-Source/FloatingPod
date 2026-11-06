package com.wi.floatingpod.server

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.util.concurrent.atomic.AtomicReference

/** 真生命周期逻辑；此 core 无 Boot/SQL/Ready issuer 权限。 */
class MainBootReadLifetimeTest:
  @Test def completionDropRevokesReadsWithoutInventingFailureOrAllowingReplay(): Unit =
    val core = new MainBootReadLifetime(Thread.currentThread(), 7L)
    core.dropForCompletion(7L)
    assertEquals(None, core.retainedFailure)
    assertThrows(classOf[IllegalArgumentException], () => core.requireCurrent(7L))
    assertThrows(classOf[IllegalArgumentException], () => core.dropForCompletion(7L))
    assertEquals(None, core.retainedFailure)

  @Test def completionDropCannotEraseOriginalFaultOrAcceptAChangedEpoch(): Unit =
    val failed = new MainBootReadLifetime(Thread.currentThread(), 7L)
    val cause = new IllegalStateException("original read fault")
    assertSame(cause, assertThrows(classOf[IllegalStateException], () => failed.fail(cause)))
    assertSame(cause, assertThrows(classOf[IllegalStateException], () => failed.dropForCompletion(7L)))
    val stale = new MainBootReadLifetime(Thread.currentThread(), 7L)
    val mismatch = assertThrows(classOf[IllegalStateException], () => stale.dropForCompletion(8L))
    assertSame(mismatch, stale.retainedFailure.get)

  @Test def unchangedEpochRemainsReadableWithoutReplacingOriginalInput(): Unit =
    val core=new MainBootReadLifetime(Thread.currentThread(),7L)
    core.requireCurrent(7L); core.requireCurrent(7L)
    assertEquals(None,core.retainedFailure)
  @Test def acceptedMutationRetiresEvenWhenQueuesWereAlreadyConsumed(): Unit =
    val core=new MainBootReadLifetime(Thread.currentThread(),7L)
    core.requireCurrent(7L)
    val retired=assertThrows(classOf[IllegalStateException], () => core.requireCurrent(8L))
    assertSame(retired,core.retainedFailure.get)
    val stack=retired.getStackTrace
    assertSame(retired,assertThrows(classOf[IllegalStateException], () => core.requireCurrent(7L)))
    assertTrue(stack.sameElements(retired.getStackTrace))
  @Test def originalFaultAndStackRemainStickyWithoutHealthyFallback(): Unit =
    val core=new MainBootReadLifetime(Thread.currentThread(),0L)
    val original=new IllegalStateException("original retained read failure")
    val stack=original.getStackTrace
    assertSame(original,assertThrows(classOf[IllegalStateException], () => core.fail(original)))
    assertSame(original,assertThrows(classOf[IllegalStateException], () => core.requireCurrent(0L)))
    assertSame(original,core.retainedFailure.get)
    assertTrue(stack.sameElements(original.getStackTrace))
  @Test def foreignThreadCannotReadOrRetireOwnerResponsibility(): Unit =
    val core=new MainBootReadLifetime(Thread.currentThread(),0L)
    val problem=new AtomicReference[Throwable]()
    val thread=new Thread(() =>
      try
        assertThrows(classOf[IllegalArgumentException], () => core.requireCurrent(1L))
        assertThrows(classOf[IllegalArgumentException], () => core.fail(new IllegalStateException("foreign")))
      catch
        case cause: Throwable => problem.set(cause)
      (),"boot-read-foreign-owner")
    var primary: Throwable=null
    try
      thread.start(); thread.join(3000L)
      assertFalse(thread.isAlive,"foreign actor deadline expired")
      if problem.get()!=null then throw problem.get()
      core.requireCurrent(0L)
      assertEquals(None,core.retainedFailure)
    catch
      case cause: Throwable => primary=cause
    finally
      if thread.isAlive then thread.interrupt()
      try
        thread.join(3000L)
        assertFalse(thread.isAlive,"foreign actor cleanup deadline expired")
      catch
        case cleanup: Throwable =>
          if primary==null then primary=cleanup
          else if cleanup ne primary then primary.addSuppressed(cleanup)
    if primary!=null then throw primary
