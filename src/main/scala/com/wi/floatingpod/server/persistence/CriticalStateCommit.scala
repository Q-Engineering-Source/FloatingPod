package com.wi.floatingpod.server.persistence

import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.storage.FloatingPodRepository

import org.apache.logging.log4j.LogManager

import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * 同步提交关键状态，要求调用方在仓储确认前不得报告业务操作成功。
 */
object CriticalStateCommit:
  private val logger = LogManager.getLogger(getClass)

  /**
   * 将关键状态直接交给仓储更新，并完整保留仓储的成功或失败结果。
   *
   * @param repository 负责权威持久化的仓储。
   * @param state 需要同步提交的关键状态。
   * @param operation 用于诊断的业务操作名称。
   * @return 仓储原样返回的更新结果。
   */
  def commit(repository: FloatingPodRepository, state: FloatingPodState, operation: String): Try[FloatingPodState] =
    logger.info(s"Committing critical state for ${state.uuid}: $operation")
    val result = repository.update(state)
    result match
      case Success(persistedState) =>
        logger.info(s"Committed critical state for ${state.uuid}: $operation, version=${persistedState.stateVersion}")
      case Failure(exception) =>
        logger.error(s"Failed to commit critical state for ${state.uuid}: $operation, error=${exception.getMessage}", exception)
    result
