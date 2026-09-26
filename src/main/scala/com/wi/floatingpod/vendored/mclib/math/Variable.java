/*
 * FloatingPod 对 Molang 求值表面的独立实现。
 * 因上游 mclib 的许可证无法取得，本文件未复制任何第三方代码。
 */
package com.wi.floatingpod.vendored.mclib.math;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * 表示由动画系统在运行时更新的具名 Molang 变量。
 */
public final class Variable implements IValue {

    /** 用于拒绝无法由 Molang 标识符解析的变量名。 */
    private static final Pattern NAME_PATTERN = Pattern.compile("[A-Za-z_][A-Za-z0-9_.]*");

    /** 变量在表达式中使用的名称。 */
    private final String name;

    /** 变量当前的数值。 */
    private volatile double value;

    /**
     * 创建具名变量。
     *
     * @param name 变量名称
     * @param value 初始数值
     */
    public Variable(String name, double value) {
        this.name = validateName(name);
        this.value = value;
    }

    /**
     * 返回变量名称，供解析器登记和诊断。
     *
     * @return 已验证的变量名称
     */
    public String getName() {
        return name;
    }

    /**
     * 更新变量的当前数值。
     *
     * @param value 新数值
     */
    public void set(double value) {
        this.value = value;
    }

    /**
     * 读取变量的当前数值。
     *
     * @return 当前数值
     */
    @Override
    public double get() {
        return value;
    }

    /**
     * 验证名称可被 Molang 标识符规则稳定解析。
     *
     * @param candidate 待验证的名称
     * @return 已验证的名称
     */
    private static String validateName(String candidate) {
        Objects.requireNonNull(candidate, "name");
        if (!NAME_PATTERN.matcher(candidate).matches()) {
            throw new IllegalArgumentException("Invalid Molang variable name: " + candidate);
        }
        return candidate;
    }
}
