package com.wi.floatingpod.probe;

import java.util.Objects;

/**
 * 表示从 Java 侧传递给 Scala 实现的不可变探针输入。
 */
public final class ProbeInput {

    /** 用于验证 Scala 能正确读取 Java 字符串访问器的稳定单元标识。 */
    private final String unitId;

    /** 用于验证 Scala 能正确读取 Java 数值访问器的有序序号。 */
    private final int sequence;

    /**
     * 创建经完整校验的不可变探针输入。
     *
     * @param unitId 非空白的稳定单元标识。
     * @param sequence 非负的探针序号。
     */
    public ProbeInput(String unitId, int sequence) {
        this.unitId = Objects.requireNonNull(unitId, "unitId");
        if (unitId.isBlank()) {
            throw new IllegalArgumentException("unitId 不能为空白");
        }
        if (sequence < 0) {
            throw new IllegalArgumentException("sequence 不能为负数");
        }
        this.sequence = sequence;
    }

    /**
     * 返回供 Scala 实现读取的稳定单元标识。
     *
     * @return 不可变的单元标识。
     */
    public String unitId() {
        return unitId;
    }

    /**
     * 返回供 Scala 实现读取的探针序号。
     *
     * @return 不可变的非负序号。
     */
    public int sequence() {
        return sequence;
    }
}
