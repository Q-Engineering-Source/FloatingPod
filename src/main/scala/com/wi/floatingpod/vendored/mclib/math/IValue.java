/*
 * FloatingPod 对 Molang 求值表面的独立实现。
 * 因上游 mclib 的许可证无法取得，本文件未复制任何第三方代码。
 */
package com.wi.floatingpod.vendored.mclib.math;

/**
 * 表示可在当前动画上下文中求值的数值。
 */
@FunctionalInterface
public interface IValue {

    /**
     * 计算当前数值。
     *
     * @return 当前求值结果
     */
    double get();
}
