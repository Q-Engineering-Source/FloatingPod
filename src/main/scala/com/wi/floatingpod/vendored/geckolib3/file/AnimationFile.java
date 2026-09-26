/*
 * 改自 GeckoLib 3.0.31：迁移包名及 Animation 类型 import 到 FloatingPod vendored 命名空间。
 * 这样动画文件容器可以随嵌入实现工作而不依赖外部 GeckoLib 类。
 */
package com.wi.floatingpod.vendored.geckolib3.file;
import java.util.Collection;
import java.util.HashMap;
import com.wi.floatingpod.vendored.geckolib3.core.builder.Animation;
public class AnimationFile {
	private HashMap<String, Animation> animations = new HashMap<>();
	public Animation getAnimation(String name) {
		return animations.get(name);
	}
	public void putAnimation(String name, Animation animation) {
		this.animations.put(name, animation);
	}
	public Collection<Animation> getAllAnimations() {
		return this.animations.values();
	}
}
