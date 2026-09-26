/*
 * 改自 GeckoLib 3.0.31：将主类和 ResourceListener import 映射到 FloatingPod 自有命名空间。
 * 这样初始化与资源重载入口可嵌入本项目，并避免加载外部 GeckoLib 主类。
 */
package com.wi.floatingpod.vendored.geckolib3;
import java.util.concurrent.FutureTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import net.minecraftforge.fml.common.FMLCommonHandler;
import net.minecraftforge.fml.relauncher.Side;
import net.minecraftforge.fml.relauncher.SideOnly;
import com.wi.floatingpod.vendored.geckolib3.resource.ResourceListener;
public class GeckoLib {
	public static final Logger LOGGER = LogManager.getLogger();
	public static final String ModID = "geckolib3";
	public static boolean hasInitialized;
	public static final String VERSION = "3.0.30";
	/**
	 * This method MUST be called in your mod's constructor or during
	 * onInitializeClient in fabric, otherwise models and animations won't be
	 * loaded.
	 */
	public static void initialize() {
		if (!hasInitialized) {
			FMLCommonHandler.callFuture(new FutureTask<>(() -> {
				if (FMLCommonHandler.instance().getSide() == Side.CLIENT) {
					doOnlyOnClient();
				}
			}, null));
		}
		hasInitialized = true;
	}
	@SideOnly(Side.CLIENT)
	private static void doOnlyOnClient() {
		ResourceListener.registerReloadListener();
	}
}
