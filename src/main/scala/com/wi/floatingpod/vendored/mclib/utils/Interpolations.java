/*
 * FloatingPod 对 Molang 求值表面的独立实现。
 * 因上游 mclib 的许可证无法取得，本文件未复制任何第三方代码。
 */
package com.wi.floatingpod.vendored.mclib.utils;

/**
 * 提供动画渲染所需的线性插值运算。
 */
public final class Interpolations {

    /** 一整圈角度的度数。 */
    private static final float FULL_TURN_DEGREES = 360.0F;

    /** 半圈角度的度数。 */
    private static final float HALF_TURN_DEGREES = 180.0F;

    /** 禁止实例化纯工具类型。 */
    private Interpolations() {
    }

    /**
     * 对普通数值执行线性插值。
     *
     * @param start 起点
     * @param end 终点
     * @param progress 插值进度
     * @return 插值结果
     */
    public static float lerp(float start, float end, float progress) {
        return start + (end - start) * progress;
    }

    /**
     * 按最短角度路径插值，避免跨越零度时绕行半圈。
     *
     * @param start 起始偏航角
     * @param end 目标偏航角
     * @param progress 插值进度
     * @return 沿最短路径插值得到的偏航角
     */
    public static float lerpYaw(float start, float end, float progress) {
        float difference = normalizeDegrees(end - start);
        return start + difference * progress;
    }

    /**
     * 将角度差归一化到半圈范围内。
     *
     * @param degrees 原始角度差
     * @return [-180, 180) 范围内的角度差
     */
    private static float normalizeDegrees(float degrees) {
        float normalized = degrees % FULL_TURN_DEGREES;
        if (normalized >= HALF_TURN_DEGREES) {
            normalized -= FULL_TURN_DEGREES;
        }
        if (normalized < -HALF_TURN_DEGREES) {
            normalized += FULL_TURN_DEGREES;
        }
        return normalized;
    }
}
