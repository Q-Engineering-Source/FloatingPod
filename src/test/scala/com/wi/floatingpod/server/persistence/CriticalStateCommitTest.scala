package com.wi.floatingpod.server.persistence

import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.storage.FloatingPodRepository

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.Appender
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.LogEvent
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * 直接验证关键提交只委派一次，并捕获实际 Log4j 事件而非检查源码文本。
 */
class CriticalStateCommitTest:
  /** 验证成功路径同步委派一次并完整返回仓储结果。 */
  @Test
  def returnsOriginalSuccessAfterOneUpdate(): Unit =
    val state = sampleState()
    val persisted = state.copy(stateVersion = 1L)
    val result = Success(persisted)
    val repository = new ControlledRepository(result)

    val actual = CriticalStateCommit.commit(repository, state, "spawn")

    assertSame(result, actual)
    assertEquals(1, repository.updateCalls)
    assertSame(state, repository.updatedState)

  /** 验证失败路径不重试、不恢复，并完整返回仓储失败结果。 */
  @Test
  def returnsOriginalFailureAfterOneUpdate(): Unit =
    val state = sampleState()
    val failure = new IllegalStateException("database unavailable")
    val result = Failure(failure)
    val repository = new ControlledRepository(result)

    val actual = CriticalStateCommit.commit(repository, state, "recall")

    assertSame(result, actual)
    assertTrue(actual.failed.get eq failure)
    assertEquals(1, repository.updateCalls)

  /** 验证开始、成功和失败均产生精确的实际 Log4j 日志消息。 */
  @Test
  def logsStartSuccessAndFailureMessages(): Unit =
    val state = sampleState()
    val events = ArrayBuffer.empty[String]
    val appender = captureAppender(events)
    val context = LogManager.getContext(false).asInstanceOf[LoggerContext]
    val logger = context.getLogger(CriticalStateCommit.getClass.getName)
    logger.addAppender(appender)
    appender.start()
    try
      CriticalStateCommit.commit(new ControlledRepository(Success(state.copy(stateVersion = 3L))), state, "spawn")
      CriticalStateCommit.commit(new ControlledRepository(Failure(new IllegalArgumentException("locked"))), state, "recall")
    finally
      logger.removeAppender(appender)
      appender.stop()

    assertEquals(
      List(
        s"Committing critical state for ${state.uuid}: spawn",
        s"Committed critical state for ${state.uuid}: spawn, version=3",
        s"Committing critical state for ${state.uuid}: recall",
        s"Failed to commit critical state for ${state.uuid}: recall, error=locked"
      ),
      events.toList
    )

  private def captureAppender(events: ArrayBuffer[String]): Appender =
    new AbstractAppender("critical-state-commit-test", null, PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY):
      override def append(event: LogEvent): Unit = events += event.getMessage.getFormattedMessage

  private def sampleState(): FloatingPodState =
    FloatingPodState(UUID.fromString("00000000-0000-0000-0000-000000000301"), None, 0L, "overworld", 0.0D, 64.0D, 0.0D, 0.0F, 0.0F, true, 1L, Some(0), com.wi.floatingpod.domain.PodOperatingMode.WorldIdle)

  private final class ControlledRepository(result: Try[FloatingPodState]) extends FloatingPodRepository:
    var updateCalls = 0
    var updatedState: FloatingPodState = null

    override def update(state: FloatingPodState): Try[FloatingPodState] =
      updateCalls += 1
      updatedState = state
      result

    override def insert(state: FloatingPodState): Try[FloatingPodState] = throw new UnsupportedOperationException()
    override def findByUuid(uuid: UUID): Try[Option[FloatingPodState]] = throw new UnsupportedOperationException()
    override def findByOwner(ownerUuid: UUID): Try[List[FloatingPodState]] = throw new UnsupportedOperationException()
    override def findAllActive(): Try[List[FloatingPodState]] = throw new UnsupportedOperationException()
    override def findAll(): Try[List[FloatingPodState]] = throw new UnsupportedOperationException()
    override def delete(uuid: UUID): Try[Boolean] = throw new UnsupportedOperationException()
    override def transaction[T](fn: => T): Try[T] = throw new UnsupportedOperationException()
    override def close(): Unit = ()
