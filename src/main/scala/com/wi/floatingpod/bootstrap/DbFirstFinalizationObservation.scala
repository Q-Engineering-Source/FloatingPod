package com.wi.floatingpod.bootstrap

import com.wi.floatingpod.server.io.IoWorkerExitHookObservation
import com.wi.floatingpod.storage.mysql.{MySqlOwnedIoCheckpoint,MySqlOwnedIoResourceObservation}

/** 仅验证原observer取得的事实；调用方仍须验证原worker join、Main回执及callback身份。 */
private[floatingpod] object DbFirstFinalizationObservation:
  def requireClosed(observed:MySqlOwnedIoCheckpoint):Unit =
    require(observed!=null,"actual schema6 finalization checkpoint required")
    val io=observed.io
    io.fatalCause.foreach(throw _)
    io.exitFailure.foreach(throw _)
    observed.controlledAfterAckFailure.foreach(value=>throw value.cause)
    observed.preparedAfterAckFailure.foreach(value=>throw value.cause)
    observed.ownedModeExecutionFailure.foreach(value=>throw value.cause)
    observed.driftExecutionFailure.foreach(value=>throw value.cause)
    require(!io.accepting && io.stopped && !io.running && io.queued==0 && io.completions==0 &&
      io.reserved==0 && !io.maintenanceScheduled && !io.maintenanceRunning &&
      io.workerExitHook.contains(IoWorkerExitHookObservation.HookReturned) && observed.retainedAllocation.isEmpty,
      "actual schema6 worker/mailbox/maintenance/static retirement incomplete")
    observed.resources match
      case Some(MySqlOwnedIoResourceObservation.DbFirstAssemblyObserved(_,failure,false))=>failure.foreach(throw _)
      case Some(MySqlOwnedIoResourceObservation.DbFirstAssemblyObserved(_,failure,true))=>
        failure.foreach(throw _)
        throw new IllegalStateException("schema6 assembly/root remains owned")
      case _=>throw new IllegalStateException("actual complete schema6 Session and assembly retirement required")
