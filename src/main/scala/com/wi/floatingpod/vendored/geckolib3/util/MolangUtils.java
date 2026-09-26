/*
 * 改自 GeckoLib 3.0.31：迁移 Molang 时间与布尔辅助类的包名到 FloatingPod 命名空间。
 * 工具行为保持不变，供独立 Molang 解析链转换动画时间。
 */
package com.wi.floatingpod.vendored.geckolib3.util;
public class MolangUtils {
	public static float normalizeTime(long timestamp) {
		return ((float) timestamp / 24000);
	}
	public static float booleanToFloat(boolean input) {
		return input ? 1.0F : 0.0F;
	}
}
