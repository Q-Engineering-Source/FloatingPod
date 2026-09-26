/*
 * 改自 GeckoLib 3.0.31：迁移 GeoModel 包名及 ModelProperties import 到 FloatingPod 命名空间。
 * 这样模型缓存保存的骨骼树由本地渲染器直接消费。
 */
package com.wi.floatingpod.vendored.geckolib3.geo.render.built;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import com.wi.floatingpod.vendored.geckolib3.geo.raw.pojo.ModelProperties;
public class GeoModel {
	public List<GeoBone> topLevelBones = new ArrayList<>();
	public ModelProperties properties;
	public Optional<GeoBone> getBone(String name) {
		for (GeoBone bone : topLevelBones) {
			GeoBone optionalBone = getBoneRecursively(name, bone);
			if (optionalBone != null) {
				return Optional.of(optionalBone);
			}
		}
		return Optional.empty();
	}
	private GeoBone getBoneRecursively(String name, GeoBone bone) {
		if (bone.name.equals(name)) {
			return bone;
		}
		for (GeoBone childBone : bone.childBones) {
			if (childBone.name.equals(name)) {
				return childBone;
			}
			GeoBone optionalBone = getBoneRecursively(name, childBone);
			if (optionalBone != null) {
				return optionalBone;
			}
		}
		return null;
	}
}
