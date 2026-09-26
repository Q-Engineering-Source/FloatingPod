package com.wi.floatingpod.probe

/**
 * 使用 Scala 3 语法实现 Java 互操作合同，并直接读取 Java 不可变输入。
 */
object ScalaInteropProbe extends InteropProbe:
  /**
   * 将 Java 输入转换为可由 Java 侧精确断言的结果。
   *
   * @param input 来自 Java 侧的不可变探针输入。
   * @return 包含单元标识、序号与 Scala 版本标记的结果。
   */
  override def evaluate(input: ProbeInput): String =
    val unitId = input.unitId()
    s"$unitId#${input.sequence()}@scala-3.8.4"
