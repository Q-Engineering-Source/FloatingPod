package com.wi.floatingpod.storage.mysql
import com.wi.floatingpod.server.io.IoOperationTag
/** Failure never declares rollback/no effect. Exact coordinator retains the full20 reserve/execute obligation. */
final case class MySqlTrackedDriftExecutionFailure(tag: IoOperationTag, request: MySqlTrackedDriftRequest,
    operationPlan: MySqlOwnedRepositoryOperationPlan, cause: Throwable):
  require(tag != null && request != null && operationPlan != null && cause != null, "exact drift failure required")
  operationPlan.requireTag(tag)
