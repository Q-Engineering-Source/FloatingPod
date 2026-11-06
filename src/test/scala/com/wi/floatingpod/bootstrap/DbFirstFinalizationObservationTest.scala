package com.wi.floatingpod.bootstrap

import com.wi.floatingpod.server.io.{IoCheckpoint,IoWorkerExitHookObservation}
import com.wi.floatingpod.storage.mysql.{MySqlOwnedIoCheckpoint,MySqlOwnedIoResourceObservation}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** 直接验证生产观察规则；这些不可变事实fixture不代表实际SQL/native关闭或stop许可。 */
final class DbFirstFinalizationObservationTest:
  private def closed:MySqlOwnedIoCheckpoint = MySqlOwnedIoCheckpoint(
    IoCheckpoint(false,0,false,0,0,true,None,Some(IoWorkerExitHookObservation.HookReturned)),
    Some(MySqlOwnedIoResourceObservation.DbFirstAssemblyObserved("original schema6 exit",None,false)))
  @Test def onlyCompleteSchema6AssemblyObservationPassesRules():Unit =
    DbFirstFinalizationObservation.requireClosed(closed)
    for resources<-Vector(None,Some(MySqlOwnedIoResourceObservation.NoVerifiedSessionOwned),
      Some(MySqlOwnedIoResourceObservation.DeploymentReceiptObserved(None,false)),
      Some(MySqlOwnedIoResourceObservation.DbFirstAssemblyObserved("incomplete",None,true))) do
      assertThrows(classOf[IllegalStateException],()=>DbFirstFinalizationObservation.requireClosed(closed.copy(resources=resources)))
  @Test def terminatedWorkerCannotEraseMailboxOrMaintenanceConsumers():Unit =
    val io=closed.io
    for incomplete<-Vector(io.copy(accepting=true),io.copy(stopped=false),io.copy(running=true),
      io.copy(queued=1),io.copy(completions=1),io.copy(reserved=1),io.copy(maintenanceScheduled=true),
      io.copy(maintenanceRunning=true),io.copy(workerExitHook=None),
      io.copy(workerExitHook=Some(IoWorkerExitHookObservation.NoHook))) do
      assertThrows(classOf[IllegalArgumentException],()=>DbFirstFinalizationObservation.requireClosed(closed.copy(io=incomplete)))
  @Test def originalFatalAndExitCausesSurviveObservation():Unit =
    val cause=new IllegalStateException("original worker failure")
    assertSame(cause,assertThrows(classOf[IllegalStateException],()=>
      DbFirstFinalizationObservation.requireClosed(closed.copy(io=closed.io.copy(fatalCause=Some(cause))))))
    assertSame(cause,assertThrows(classOf[IllegalStateException],()=>
      DbFirstFinalizationObservation.requireClosed(closed.copy(io=closed.io.copy(exitFailure=Some(cause))))))
  @Test def originalAssemblyFailureCannotBecomeClosedWhenRetainedFlagIsFalse():Unit =
    val cause=new IllegalStateException("original root cleanup failure")
    for retained<-Vector(false,true) do
      assertSame(cause,assertThrows(classOf[IllegalStateException],()=>DbFirstFinalizationObservation.requireClosed(
        closed.copy(resources=Some(MySqlOwnedIoResourceObservation.DbFirstAssemblyObserved("failed",Some(cause),retained))))))
