/*
 * 改自 GeckoLib 3.0.31：迁移资源缓存及加载链 import 到 FloatingPod 命名空间。
 * 已知模型和动画资源通过 IResourceManager 延迟解析，避免访问资源包文件系统。
 */
package com.wi.floatingpod.vendored.geckolib3.resource;

import java.util.HashMap;

import net.minecraft.client.resources.IResourceManager;
import net.minecraft.client.resources.IResourceManagerReloadListener;
import net.minecraft.util.ResourceLocation;

import com.wi.floatingpod.vendored.geckolib3.GeckoLib;
import com.wi.floatingpod.vendored.geckolib3.core.molang.MolangParser;
import com.wi.floatingpod.vendored.geckolib3.core.molang.MolangRegistrar;
import com.wi.floatingpod.vendored.geckolib3.file.AnimationFile;
import com.wi.floatingpod.vendored.geckolib3.file.AnimationFileLoader;
import com.wi.floatingpod.vendored.geckolib3.file.GeoModelLoader;
import com.wi.floatingpod.vendored.geckolib3.geo.render.built.GeoModel;

public class GeckoLibCache implements IResourceManagerReloadListener {
	private final AnimationFileLoader animationLoader;
	private final GeoModelLoader modelLoader;
	private final HashMap<ResourceLocation, AnimationFile> animations = new HashMap<>();
	private final HashMap<ResourceLocation, GeoModel> geoModels = new HashMap<>();
	public final MolangParser parser = new MolangParser();
	private IResourceManager resourceManager;
	private long resourceGeneration;

	protected GeckoLibCache() {
		this.animationLoader = new AnimationFileLoader();
		this.modelLoader = new GeoModelLoader();
		MolangRegistrar.registerVars(parser);
	}

	public static GeckoLibCache getInstance() {
		return InstanceHolder.INSTANCE;
	}

	/**
	 * 返回动画缓存的旧版快照。修改返回的 map 不会影响当前缓存。
	 *
	 * @deprecated 正常资源访问请使用 {@link #getAnimationFile(ResourceLocation)}。
	 */
	@Deprecated
	public synchronized HashMap<ResourceLocation, AnimationFile> getAnimations() {
		ensureInitialized();
		return new HashMap<>(animations);
	}

	/**
	 * 返回模型缓存的旧版快照。修改返回的 map 不会影响当前缓存。
	 *
	 * @deprecated 正常资源访问请使用 {@link #getGeoModel(ResourceLocation)}。
	 */
	@Deprecated
	public synchronized HashMap<ResourceLocation, GeoModel> getGeoModels() {
		ensureInitialized();
		return new HashMap<>(geoModels);
	}

	public synchronized GeoModel getGeoModel(ResourceLocation location) {
		requireLocation("model", location);
		ensureInitialized();
		IResourceManager currentResourceManager = requireResourceManager("model", location);
		GeoModel cachedModel = geoModels.get(location);
		if (cachedModel != null) {
			return cachedModel;
		}
		try {
			GeoModel loadedModel = modelLoader.loadModel(currentResourceManager, location);
			geoModels.put(location, loadedModel);
			return loadedModel;
		} catch (RuntimeException e) {
			throw loadFailure("model", location, e);
		}
	}

	public synchronized AnimationFile getAnimationFile(ResourceLocation location) {
		requireLocation("animation", location);
		ensureInitialized();
		IResourceManager currentResourceManager = requireResourceManager("animation", location);
		AnimationFile cachedAnimation = animations.get(location);
		if (cachedAnimation != null) {
			return cachedAnimation;
		}
		try {
			AnimationFile loadedAnimation = animationLoader.loadAllAnimations(parser, location, currentResourceManager);
			animations.put(location, loadedAnimation);
			return loadedAnimation;
		} catch (RuntimeException e) {
			throw loadFailure("animation", location, e);
		}
	}

	@Override
	public synchronized void onResourceManagerReload(IResourceManager resourceManager) {
		if (resourceManager == null) {
			throw new IllegalArgumentException("GeckoLib resource manager must not be null.");
		}
		this.resourceManager = resourceManager;
		this.resourceGeneration += 1;
		this.animations.clear();
		this.geoModels.clear();
	}

	private void ensureInitialized() {
		if (!GeckoLib.hasInitialized) {
			throw new RuntimeException("GeckoLib was never initialized! Please read the documentation!");
		}
	}

	private void requireLocation(String resourceType, ResourceLocation location) {
		if (location == null) {
			String message = "Cannot load GeckoLib " + resourceType + " resource because its ResourceLocation is null.";
			GeckoLib.LOGGER.error(message);
			throw new IllegalArgumentException(message);
		}
	}

	private IResourceManager requireResourceManager(String resourceType, ResourceLocation location) {
		if (resourceManager == null) {
			String message = "Cannot load GeckoLib " + resourceType + " resource \"" + location
					+ "\" because the client resource manager is unavailable; wait for the first client resource reload.";
			GeckoLib.LOGGER.error(message);
			throw new IllegalStateException(message);
		}
		return resourceManager;
	}

	private RuntimeException loadFailure(String resourceType, ResourceLocation location, RuntimeException cause) {
		String message = "Could not load GeckoLib " + resourceType + " resource \"" + location
				+ "\" for resource generation " + resourceGeneration + ".";
		GeckoLib.LOGGER.error(message, cause);
		return new RuntimeException(message, cause);
	}

	private static final class InstanceHolder {
		private static final GeckoLibCache INSTANCE = new GeckoLibCache();
	}
}
