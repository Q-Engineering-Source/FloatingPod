package com.wi.floatingpod.probe

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/**
 * 验证 Scala 3 object 能通过 Java 定义的 InteropProbe 接口参与 JUnit 测试。
 */
class ScalaInteropProbeTest:
  /**
   * 验证接口类型引用保留 Scala 实现的精确互操作结果。
   */
  @Test
  def evaluatesThroughInteropProbeInterface(): Unit =
    val probe: InteropProbe = ScalaInteropProbe

    assertEquals("drone-001#7@scala-3.8.4", probe.evaluate(new ProbeInput("drone-001", 7)))
