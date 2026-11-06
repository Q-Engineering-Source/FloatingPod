package com.wi.floatingpod.vendored.geckolib3.resource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import net.minecraft.client.resources.IResource;
import net.minecraft.client.resources.IResourceManager;
import net.minecraft.client.resources.data.IMetadataSection;
import net.minecraft.util.ResourceLocation;

import com.wi.floatingpod.vendored.geckolib3.GeckoLib;
import com.wi.floatingpod.vendored.geckolib3.core.IAnimatable;
import com.wi.floatingpod.vendored.geckolib3.core.builder.Animation;
import com.wi.floatingpod.vendored.geckolib3.core.manager.AnimationData;
import com.wi.floatingpod.vendored.geckolib3.core.manager.AnimationFactory;
import com.wi.floatingpod.vendored.geckolib3.file.AnimationFile;
import com.wi.floatingpod.vendored.geckolib3.geo.render.built.GeoModel;
import com.wi.floatingpod.vendored.geckolib3.model.AnimatedGeoModel;
import com.wi.floatingpod.vendored.geckolib3.model.provider.GeoModelProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 GeckoLib 缓存按资源重载代际延迟解析已知模型和动画资源。
 */
@TestMethodOrder(OrderAnnotation.class)
public class GeckoLibCacheTest {
	private static final ResourceLocation MODEL_LOCATION = new ResourceLocation("floatingpod",
			"geo/assembly/core/pod_core_v0_00_01.geo.json");
	private static final ResourceLocation ANIMATION_LOCATION = new ResourceLocation("floatingpod",
			"animations/assembly/core/pod_core_v0_00_01.animation.json");
	private boolean previousInitialized;

	@BeforeEach
	void enableGeckoLibCacheAccess() {
		previousInitialized = GeckoLib.hasInitialized;
		GeckoLib.hasInitialized = true;
	}

	@AfterEach
	void restoreGeckoLibInitializationState() {
		GeckoLib.hasInitialized = previousInitialized;
	}

	@Test
	@Order(2)
	void loadsKnownModelAndAnimationOncePerResourceManagerGeneration() {
		GeckoLibCache cache = new GeckoLibCache();
		CountingResourceManager resourceManager = new CountingResourceManager();
		cache.onResourceManagerReload(resourceManager);

		assertEquals(0, resourceManager.requestCount(MODEL_LOCATION));
		assertEquals(0, resourceManager.requestCount(ANIMATION_LOCATION));

		GeoModel model = cache.getGeoModel(MODEL_LOCATION);
		AnimationFile animation = cache.getAnimationFile(ANIMATION_LOCATION);

		assertNotNull(model);
		assertNotNull(animation);
		assertSame(model, cache.getGeoModel(MODEL_LOCATION));
		assertSame(animation, cache.getAnimationFile(ANIMATION_LOCATION));
		assertSame(model, cache.getGeoModels().get(MODEL_LOCATION));
		assertSame(animation, cache.getAnimations().get(ANIMATION_LOCATION));
		assertEquals(1, resourceManager.requestCount(MODEL_LOCATION));
		assertEquals(1, resourceManager.requestCount(ANIMATION_LOCATION));
	}

	@Test
	@Order(3)
	void failsBeforeTheFirstResourceReloadWithTheRequestedResourceLocation() {
		GeckoLibCache cache = new GeckoLibCache();

		IllegalStateException failure = assertThrows(IllegalStateException.class, () -> cache.getGeoModel(MODEL_LOCATION));

		assertTrue(failure.getMessage().contains(MODEL_LOCATION.toString()));
		assertTrue(failure.getMessage().contains("client resource manager is unavailable"));
	}

	@Test
	@Order(4)
	void reloadInvalidatesModelAndAnimationResultsForTheNextResourceManager() {
		GeckoLibCache cache = new GeckoLibCache();
		CountingResourceManager firstResourceManager = new CountingResourceManager();
		cache.onResourceManagerReload(firstResourceManager);
		GeoModel firstModel = cache.getGeoModel(MODEL_LOCATION);
		AnimationFile firstAnimation = cache.getAnimationFile(ANIMATION_LOCATION);

		CountingResourceManager secondResourceManager = new CountingResourceManager();
		cache.onResourceManagerReload(secondResourceManager);
		GeoModel secondModel = cache.getGeoModel(MODEL_LOCATION);
		AnimationFile secondAnimation = cache.getAnimationFile(ANIMATION_LOCATION);

		assertNotSame(firstModel, secondModel);
		assertNotSame(firstAnimation, secondAnimation);
		assertEquals(1, firstResourceManager.requestCount(MODEL_LOCATION));
		assertEquals(1, firstResourceManager.requestCount(ANIMATION_LOCATION));
		assertEquals(1, secondResourceManager.requestCount(MODEL_LOCATION));
		assertEquals(1, secondResourceManager.requestCount(ANIMATION_LOCATION));
	}

	@Test
	@Order(5)
	void missingKnownResourcesFailWithTheirResourceLocation() {
		GeckoLibCache cache = new GeckoLibCache();
		CountingResourceManager resourceManager = new CountingResourceManager();
		ResourceLocation missingModel = new ResourceLocation("floatingpod", "geo/missing.geo.json");
		ResourceLocation missingAnimation = new ResourceLocation("floatingpod", "animations/missing.animation.json");
		cache.onResourceManagerReload(resourceManager);

		RuntimeException modelFailure = assertThrows(RuntimeException.class, () -> cache.getGeoModel(missingModel));
		RuntimeException animationFailure = assertThrows(RuntimeException.class,
				() -> cache.getAnimationFile(missingAnimation));

		assertTrue(modelFailure.getMessage().contains(missingModel.toString()));
		assertTrue(animationFailure.getMessage().contains(missingAnimation.toString()));
		assertNotNull(modelFailure.getCause());
		assertNotNull(animationFailure.getCause());
		assertEquals(1, resourceManager.requestCount(missingModel));
		assertEquals(1, resourceManager.requestCount(missingAnimation));
	}

	@Test
	@Order(6)
	void retainedLegacySnapshotsCannotSeedTheNextResourceGeneration() {
		GeckoLibCache cache = new GeckoLibCache();
		CountingResourceManager firstResourceManager = new CountingResourceManager();
		cache.onResourceManagerReload(firstResourceManager);
		GeoModel firstModel = cache.getGeoModel(MODEL_LOCATION);
		AnimationFile firstAnimation = cache.getAnimationFile(ANIMATION_LOCATION);
		HashMap<ResourceLocation, GeoModel> retainedModels = cache.getGeoModels();
		HashMap<ResourceLocation, AnimationFile> retainedAnimations = cache.getAnimations();

		CountingResourceManager secondResourceManager = new CountingResourceManager();
		cache.onResourceManagerReload(secondResourceManager);
		retainedModels.put(MODEL_LOCATION, firstModel);
		retainedAnimations.put(ANIMATION_LOCATION, firstAnimation);

		GeoModel secondModel = cache.getGeoModel(MODEL_LOCATION);
		AnimationFile secondAnimation = cache.getAnimationFile(ANIMATION_LOCATION);

		assertSame(secondModel, cache.getGeoModel(MODEL_LOCATION));
		assertSame(secondAnimation, cache.getAnimationFile(ANIMATION_LOCATION));
		HashMap<ResourceLocation, GeoModel> currentModels = cache.getGeoModels();
		HashMap<ResourceLocation, AnimationFile> currentAnimations = cache.getAnimations();
		assertEquals(1, currentModels.size());
		assertEquals(1, currentAnimations.size());
		assertSame(secondModel, currentModels.get(MODEL_LOCATION));
		assertSame(secondAnimation, currentAnimations.get(ANIMATION_LOCATION));
		assertEquals(1, firstResourceManager.requestCount(MODEL_LOCATION));
		assertEquals(1, firstResourceManager.requestCount(ANIMATION_LOCATION));
		assertEquals(1, secondResourceManager.requestCount(MODEL_LOCATION));
		assertEquals(1, secondResourceManager.requestCount(ANIMATION_LOCATION));
	}

	@Test
	@Order(1)
	void singletonReturnsTheSameCacheToConcurrentCallers() throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(8);
		CountDownLatch callersReady = new CountDownLatch(8);
		CountDownLatch beginLookup = new CountDownLatch(1);
		try {
			List<Future<GeckoLibCache>> lookups = new ArrayList<>();
			for (int index = 0; index < 8; index++) {
				lookups.add(executor.submit(() -> {
					callersReady.countDown();
					if (!beginLookup.await(10, TimeUnit.SECONDS)) {
						throw new IllegalStateException("Timed out waiting to start the singleton lookup.");
					}
					return GeckoLibCache.getInstance();
				}));
			}
			assertTrue(callersReady.await(10, TimeUnit.SECONDS));
			beginLookup.countDown();

			GeckoLibCache singleton = lookups.get(0).get(10, TimeUnit.SECONDS);
			for (Future<GeckoLibCache> lookup : lookups) {
				assertSame(singleton, lookup.get(10, TimeUnit.SECONDS));
			}
		} finally {
			executor.shutdownNow();
			assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
		}
	}

	@Test
	@Order(7)
	void geoModelProviderLoadsKnownModelsThroughTheLazyCache() {
		GeckoLibCache cache = GeckoLibCache.getInstance();
		CountingResourceManager resourceManager = new CountingResourceManager();
		cache.onResourceManagerReload(resourceManager);

		GeoModel model = new TestGeoModelProvider().getModel(MODEL_LOCATION);

		assertNotNull(model);
		assertSame(model, cache.getGeoModels().get(MODEL_LOCATION));
		assertEquals(1, resourceManager.requestCount(MODEL_LOCATION));
	}

	@Test
	@Order(8)
	void animatedGeoModelLoadsKnownAnimationsThroughTheLazyCache() {
		GeckoLibCache cache = GeckoLibCache.getInstance();
		CountingResourceManager resourceManager = new CountingResourceManager();
		cache.onResourceManagerReload(resourceManager);

		Animation animation = new TestAnimatedGeoModel().getAnimation("idle", new TestAnimatable());

		assertNotNull(animation);
		assertEquals(1, resourceManager.requestCount(ANIMATION_LOCATION));
	}

	@Test
	@Order(9)
	void reloadCannotPublishACompletedLoadFromThePreviousResourceManager() throws Exception {
		GeckoLibCache cache = new GeckoLibCache();
		BlockingResourceManager firstResourceManager = new BlockingResourceManager();
		CountingResourceManager secondResourceManager = new CountingResourceManager();
		cache.onResourceManagerReload(firstResourceManager);

		AtomicReference<GeoModel> firstModel = new AtomicReference<>();
		AtomicReference<Throwable> firstLoadFailure = new AtomicReference<>();
		AtomicReference<Throwable> reloadFailure = new AtomicReference<>();
		Thread firstLoad = new Thread(() -> {
			try {
				firstModel.set(cache.getGeoModel(MODEL_LOCATION));
			} catch (Throwable throwable) {
				firstLoadFailure.set(throwable);
			}
		}, "gecko-first-resource-load");
		Thread reload = new Thread(() -> {
			try {
				cache.onResourceManagerReload(secondResourceManager);
			} catch (Throwable throwable) {
				reloadFailure.set(throwable);
			}
		}, "gecko-resource-reload");
		try {
			firstLoad.start();
			firstResourceManager.awaitModelRequest();

			reload.start();
			awaitReloadThreadBlocked(reload);
			assertEquals(Thread.State.BLOCKED, reload.getState());

			firstResourceManager.allowModelLoadToComplete();
			firstLoad.join(TimeUnit.SECONDS.toMillis(10));
			reload.join(TimeUnit.SECONDS.toMillis(10));
			assertFalse(firstLoad.isAlive());
			assertFalse(reload.isAlive());
			assertNull(firstLoadFailure.get());
			assertNull(reloadFailure.get());
			assertNotNull(firstModel.get());
			GeoModel secondModel = cache.getGeoModel(MODEL_LOCATION);

			assertNotSame(firstModel.get(), secondModel);
			assertEquals(1, firstResourceManager.requestCount(MODEL_LOCATION));
			assertEquals(1, secondResourceManager.requestCount(MODEL_LOCATION));
		} finally {
			firstResourceManager.allowModelLoadToComplete();
			firstLoad.join(TimeUnit.SECONDS.toMillis(10));
			reload.join(TimeUnit.SECONDS.toMillis(10));
		}
	}

	@Test
	@Order(10)
	void nullModelLocationFailsBeforeResourceManagerAccess() {
		GeckoLibCache cache = new GeckoLibCache();
		CountingResourceManager resourceManager = new CountingResourceManager();
		cache.onResourceManagerReload(resourceManager);

		IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> cache.getGeoModel(null));

		assertTrue(failure.getMessage().contains("model"));
		assertTrue(failure.getMessage().contains("ResourceLocation is null"));
		assertEquals(0, resourceManager.requestCount(MODEL_LOCATION));
		assertEquals(0, resourceManager.totalRequestCount());
	}

	@Test
	@Order(11)
	void nullAnimationLocationFailsBeforeResourceManagerAccess() {
		GeckoLibCache cache = new GeckoLibCache();
		CountingResourceManager resourceManager = new CountingResourceManager();
		cache.onResourceManagerReload(resourceManager);

		IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
				() -> cache.getAnimationFile(null));

		assertTrue(failure.getMessage().contains("animation"));
		assertTrue(failure.getMessage().contains("ResourceLocation is null"));
		assertEquals(0, resourceManager.requestCount(ANIMATION_LOCATION));
		assertEquals(0, resourceManager.totalRequestCount());
	}

	private static void awaitReloadThreadBlocked(Thread thread) {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (thread.getState() != Thread.State.BLOCKED) {
			if (!thread.isAlive()) {
				throw new AssertionError("Thread " + thread.getName()
						+ " terminated before reaching BLOCKED");
			}
			if (System.nanoTime() >= deadline) {
				throw new AssertionError("Thread " + thread.getName() + " did not reach BLOCKED"
						+ "; current state is " + thread.getState());
			}
			Thread.yield();
		}
	}

	/**
	 * 为真实打包资源提供计数资源管理器，以验证缓存是否重复请求底层资源。
	 */
	private static class CountingResourceManager implements IResourceManager {
		private final Map<ResourceLocation, Integer> requestCounts = new HashMap<>();

		@Override
		public Set<String> getResourceDomains() {
			return Collections.singleton("floatingpod");
		}

		@Override
		public IResource getResource(ResourceLocation location) throws IOException {
			requestCounts.merge(location, 1, Integer::sum);
			return new TestResource(location, readResource(location));
		}

		@Override
		public List<IResource> getAllResources(ResourceLocation location) throws IOException {
			return Collections.singletonList(getResource(location));
		}

		int requestCount(ResourceLocation location) {
			return requestCounts.getOrDefault(location, 0);
		}

		int totalRequestCount() {
			return requestCounts.values().stream().mapToInt(Integer::intValue).sum();
		}

		private byte[] readResource(ResourceLocation location) throws IOException {
			String path = "assets/" + location.getNamespace() + "/" + location.getPath();
			try (InputStream inputStream = GeckoLibCacheTest.class.getClassLoader().getResourceAsStream(path)) {
				if (inputStream == null) {
					throw new IOException("Missing test resource: " + path);
				}
				return inputStream.readAllBytes();
			}
		}
	}

	/**
	 * 在底层资源读取中暂停，以验证重载不能与同一缓存的加载操作交错。
	 */
	private static final class BlockingResourceManager extends CountingResourceManager {
		private final CountDownLatch modelRequest = new CountDownLatch(1);
		private final CountDownLatch allowModelLoad = new CountDownLatch(1);

		@Override
		public IResource getResource(ResourceLocation location) throws IOException {
			IResource resource = super.getResource(location);
			if (MODEL_LOCATION.equals(location)) {
				modelRequest.countDown();
				awaitModelLoadPermission();
			}
			return resource;
		}

		void awaitModelRequest() throws InterruptedException {
			assertTrue(modelRequest.await(10, TimeUnit.SECONDS));
		}

		void allowModelLoadToComplete() {
			allowModelLoad.countDown();
		}

		private void awaitModelLoadPermission() throws IOException {
			try {
				if (!allowModelLoad.await(10, TimeUnit.SECONDS)) {
					throw new IOException("Timed out waiting to continue the test model load.");
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IOException("Interrupted while waiting to continue the test model load.", e);
			}
		}
	}

	/**
	 * 使用实际模型提供器接口验证延迟缓存访问不会回退到空 map 查询。
	 */
	private static final class TestGeoModelProvider extends GeoModelProvider<Object> {
		@Override
		public ResourceLocation getModelLocation(Object object) {
			return MODEL_LOCATION;
		}

		@Override
		public ResourceLocation getTextureLocation(Object object) {
			return MODEL_LOCATION;
		}
	}

	/**
	 * 使用实际动画模型入口验证动画资源通过延迟缓存加载。
	 */
	private static final class TestAnimatedGeoModel extends AnimatedGeoModel<TestAnimatable> {
		@Override
		public ResourceLocation getModelLocation(TestAnimatable object) {
			return MODEL_LOCATION;
		}

		@Override
		public ResourceLocation getTextureLocation(TestAnimatable object) {
			return MODEL_LOCATION;
		}

		@Override
		public ResourceLocation getAnimationFileLocation(TestAnimatable object) {
			return ANIMATION_LOCATION;
		}
	}

	/**
	 * 为动画模型入口提供最小且完整的 IAnimatable 实现。
	 */
	private static final class TestAnimatable implements IAnimatable {
		private final AnimationFactory factory = new AnimationFactory(this);

		@Override
		public void registerControllers(AnimationData data) {
		}

		@Override
		public AnimationFactory getFactory() {
			return factory;
		}
	}

	/**
	 * 将 classpath 资源适配为 GeckoLib 加载器要求的 IResource。
	 */
	private static final class TestResource implements IResource {
		private final ResourceLocation location;
		private final byte[] content;

		TestResource(ResourceLocation location, byte[] content) {
			this.location = location;
			this.content = content;
		}

		@Override
		public ResourceLocation getResourceLocation() {
			return location;
		}

		@Override
		public InputStream getInputStream() {
			return new ByteArrayInputStream(content);
		}

		@Override
		public boolean hasMetadata() {
			return false;
		}

		@Override
		public <T extends IMetadataSection> T getMetadata(String sectionName) {
			return null;
		}

		@Override
		public String getResourcePackName() {
			return "floatingpod-test";
		}

		@Override
		public void close() {
		}
	}
}
