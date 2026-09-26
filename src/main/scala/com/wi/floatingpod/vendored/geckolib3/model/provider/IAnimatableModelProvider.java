/*
 * 改自 GeckoLib 3.0.31：迁移动画资源提供器接口的包名到 FloatingPod 命名空间。
 * 接口签名保持不变，供本地动画模型链解析资源位置。
 */
package com.wi.floatingpod.vendored.geckolib3.model.provider;
import net.minecraft.util.ResourceLocation;
public interface IAnimatableModelProvider<E> {
	/**
	 * This resource location needs to point to a json file of your animation file,
	 * i.e. "geckolib:animations/frog_animation.json"
	 *
	 * @return the animation file location
	 */
	ResourceLocation getAnimationFileLocation(E animatable);
}
