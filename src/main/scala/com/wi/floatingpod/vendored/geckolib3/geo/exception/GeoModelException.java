/*
 * 改自 GeckoLib 3.0.31：迁移异常类型的包名到 FloatingPod vendored 命名空间。
 * 这样模型加载错误仍保留原始资源定位信息，并与嵌入的 geo 链路一致。
 */
package com.wi.floatingpod.vendored.geckolib3.geo.exception;
import net.minecraft.util.ResourceLocation;
@SuppressWarnings("serial")
public class GeoModelException extends RuntimeException {
	public GeoModelException(ResourceLocation fileLocation, String message) {
		super(fileLocation + ": " + message);
	}
	public GeoModelException(ResourceLocation fileLocation, String message, Throwable cause) {
		super(fileLocation + ": " + message, cause);
	}
}
