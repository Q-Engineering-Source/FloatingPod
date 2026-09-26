/*
 * 改自 GeckoLib 3.0.31：迁移资源监听器包名并指向 FloatingPod 的 GeckoLibCache。
 * 这样客户端资源重载仍注册同一缓存，但不会加载外部 GeckoLib 类。
 */
package com.wi.floatingpod.vendored.geckolib3.resource;
import net.minecraft.client.Minecraft;
import net.minecraft.client.resources.IReloadableResourceManager;
public class ResourceListener {
	public static void registerReloadListener() {
		if (Minecraft.getMinecraft().getResourceManager() == null) {
			throw new RuntimeException(
					"GeckoLib was initialized too early! If you are on fabric, please read the wiki on when to initialize!");
		}
		IReloadableResourceManager reloadable = (IReloadableResourceManager) Minecraft.getMinecraft()
				.getResourceManager();
		reloadable.registerReloadListener(GeckoLibCache.getInstance());
	}
}
