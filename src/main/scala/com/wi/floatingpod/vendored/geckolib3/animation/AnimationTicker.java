/*
 * 改自 GeckoLib 3.0.31：将包名和 AnimationData import 映射到 FloatingPod 自有命名空间。
 * 这样可以在不依赖外部 GeckoLib 的情况下保留客户端动画 tick 行为。
 */
package com.wi.floatingpod.vendored.geckolib3.animation;
import net.minecraft.client.Minecraft;
import net.minecraftforge.fml.common.eventhandler.SubscribeEvent;
import net.minecraftforge.fml.common.gameevent.TickEvent;
import com.wi.floatingpod.vendored.geckolib3.core.manager.AnimationData;
public class AnimationTicker {
	private final AnimationData data;
	public AnimationTicker(AnimationData data) {
		this.data = data;
	}
	@SubscribeEvent
	public void tickEvent(TickEvent.ClientTickEvent event) {
		if (Minecraft.getMinecraft().isGamePaused() && !data.shouldPlayWhilePaused) {
			return;
		}
		if (event.phase == TickEvent.Phase.END) {
			data.tick++;
		}
	}
}
