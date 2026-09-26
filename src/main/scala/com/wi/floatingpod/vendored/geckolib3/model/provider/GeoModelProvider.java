/*
 * 改自 GeckoLib 3.0.31：迁移模型提供器及 GeckoLibCache import 到 FloatingPod 命名空间。
 * 这样资源定位仍访问共享模型缓存，但不再绑定外部 GeckoLib 包。
 */
package com.wi.floatingpod.vendored.geckolib3.model.provider;
import net.minecraft.util.ResourceLocation;
import com.wi.floatingpod.vendored.geckolib3.geo.render.built.GeoModel;
import com.wi.floatingpod.vendored.geckolib3.resource.GeckoLibCache;
public abstract class GeoModelProvider<T> {
	public double seekTime;
	public double lastGameTickTime;
	public boolean shouldCrashOnMissing = false;
	public GeoModel getModel(ResourceLocation location) {
		return GeckoLibCache.getInstance().getGeoModel(location);
	}
	public abstract ResourceLocation getModelLocation(T object);
	public abstract ResourceLocation getTextureLocation(T object);
}
