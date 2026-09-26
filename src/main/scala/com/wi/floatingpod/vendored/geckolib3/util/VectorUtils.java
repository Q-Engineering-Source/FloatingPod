/*
 * 改自 GeckoLib 3.0.31：迁移向量工具及 commons/vecmath import 到 FloatingPod 命名空间。
 * 保留数组长度 Fail Fast 校验，供本地几何构建器创建向量。
 */
package com.wi.floatingpod.vendored.geckolib3.util;
import javax.vecmath.Vector3d;
import javax.vecmath.Vector3f;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.Validate;
public class VectorUtils {
	public static Vector3d fromArray(double[] array) {
		Validate.validIndex(ArrayUtils.toObject(array), 2);
		return new Vector3d(array[0], array[1], array[2]);
	}
	public static Vector3f fromArray(float[] array) {
		Validate.validIndex(ArrayUtils.toObject(array), 2);
		return new Vector3f(array[0], array[1], array[2]);
	}
	public static Vector3f convertDoubleToFloat(Vector3d vector) {
		return new Vector3f((float) vector.x, (float) vector.y, (float) vector.z);
	}
	public static Vector3d convertFloatToDouble(Vector3f vector) {
		return new Vector3d(vector.getX(), vector.getY(), vector.getZ());
	}
}
