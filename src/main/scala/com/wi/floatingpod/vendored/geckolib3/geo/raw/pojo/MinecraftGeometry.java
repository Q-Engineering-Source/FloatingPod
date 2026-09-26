/*
 * 改自 GeckoLib 3.0.31：将 MinecraftGeometry 字段注解迁移为 Gson 并改用 FloatingPod 包名。
 * 该调整保持 minecraft:geometry 等 JSON 键名，移除 Jackson 运行时需求。
 */
package com.wi.floatingpod.vendored.geckolib3.geo.raw.pojo;

import com.google.gson.annotations.SerializedName;

/**
 * 单个 Minecraft 几何定义。
 */
public class MinecraftGeometry {
	@SerializedName("bones")
	private Bone[] bones;
	@SerializedName("cape")
	private String cape;
	@SerializedName("description")
	private ModelProperties modelProperties;

	/**
	 * Bones define the 'skeleton' of the mob: the parts that can be animated, and
	 * to which geometry and other bones are attached.
	 */
	public Bone[] getBones() {
		return bones;
	}

	public void setBones(Bone[] value) {
		this.bones = value;
	}

	public String getCape() {
		return cape;
	}

	public void setCape(String value) {
		this.cape = value;
	}

	public ModelProperties getProperties() {
		return modelProperties;
	}

	public void setProperties(ModelProperties value) {
		this.modelProperties = value;
	}
}
