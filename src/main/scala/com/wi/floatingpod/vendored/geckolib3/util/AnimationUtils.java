/*
 * Copyright (c) 2020.
 * Author: Bernie G. (Gecko)
 */
/*
 * 改自 GeckoLib 3.0.31：迁移工具类包名及模型/渲染器 import 到 FloatingPod 命名空间。
 * 这样 tick 转换和实体渲染辅助方法由本地 vendored 链路提供。
 */
package com.wi.floatingpod.vendored.geckolib3.util;
import net.minecraft.client.Minecraft;
import net.minecraft.client.renderer.entity.Render;
import net.minecraft.client.renderer.entity.RenderManager;
import net.minecraft.entity.Entity;
import com.wi.floatingpod.vendored.geckolib3.model.provider.GeoModelProvider;
import com.wi.floatingpod.vendored.geckolib3.renderers.geo.IGeoRenderer;
public class AnimationUtils {
	public static double convertTicksToSeconds(double ticks) {
		return ticks / 20;
	}
	public static double convertSecondsToTicks(double seconds) {
		return seconds * 20;
	}
	/**
	 * Gets the renderer for an entity
	 */
	public static <T extends Entity> Render<T> getRenderer(T entity) {
		RenderManager renderManager = Minecraft.getMinecraft().getRenderManager();
		return renderManager.getEntityRenderObject(entity);
	}
	@SuppressWarnings("rawtypes")
	public static <T extends Entity> GeoModelProvider getGeoModelForEntity(T entity) {
		Render<T> entityRenderer = getRenderer(entity);
		if (entityRenderer instanceof IGeoRenderer) {
			return ((IGeoRenderer<?>) entityRenderer).getGeoModelProvider();
		}
		return null;
	}
}
