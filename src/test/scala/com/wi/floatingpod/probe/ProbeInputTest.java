package com.wi.floatingpod.probe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * 直接验证 ProbeInput 对 Java 调用方公开的访问器与输入校验契约。
 */
class ProbeInputTest {

    /**
     * 验证有效输入会保留单元标识和非负序号，供 Scala 实现读取。
     */
    @Test
    void exposesValidatedAccessors() {
        ProbeInput input = new ProbeInput("drone-001", 7);

        assertEquals("drone-001", input.unitId());
        assertEquals(7, input.sequence());
    }

    /**
     * 验证零序号属于允许的边界值。
     */
    @Test
    void acceptsZeroSequence() {
        ProbeInput input = new ProbeInput("drone-001", 0);

        assertEquals(0, input.sequence());
    }

    /**
     * 验证空引用单元标识会以可诊断字段名失败。
     */
    @Test
    void rejectsNullUnitId() {
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new ProbeInput(null, 7)
        );

        assertTrue(exception.getMessage().contains("unitId"));
    }

    /**
     * 验证空字符串单元标识会以可诊断字段名失败。
     */
    @Test
    void rejectsEmptyUnitId() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new ProbeInput("", 7)
        );

        assertTrue(exception.getMessage().contains("unitId"));
    }

    /**
     * 验证纯空白单元标识会以可诊断字段名失败。
     */
    @Test
    void rejectsBlankUnitId() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new ProbeInput("   ", 7)
        );

        assertTrue(exception.getMessage().contains("unitId"));
    }

    /**
     * 验证负序号会以可诊断字段名失败。
     */
    @Test
    void rejectsNegativeSequence() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new ProbeInput("drone-001", -1)
        );

        assertTrue(exception.getMessage().contains("sequence"));
    }
}
