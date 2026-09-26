package com.wi.floatingpod.probe;

/**
 * 定义 Java 与 Scala 之间的最小互操作合同，使业务边界依赖接口而非 Scala 具体实现。
 */
@FunctionalInterface
public interface InteropProbe {

    /**
     * 使用 Java 侧输入生成可观察结果。
     *
     * @param input 不可变的探针输入。
     * @return Scala 实现计算出的结果。
     */
    String evaluate(ProbeInput input);
}
