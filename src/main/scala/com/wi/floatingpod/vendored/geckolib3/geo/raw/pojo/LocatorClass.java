/*
 * 改自 GeckoLib 3.0.31：将定位器对象字段迁移为 Gson 注解并置于 FloatingPod 包名。
 * 这样定位器的 JSON 命名保持兼容，运行时不再需要 Jackson。
 */
package com.wi.floatingpod.vendored.geckolib3.geo.raw.pojo;

import com.google.gson.annotations.SerializedName;

/**
 * 以对象形式定义的骨骼定位器。
 */
public class LocatorClass {
	@SerializedName("ignore_inherited_scale")
	private Boolean ignoreInheritedScale;
	@SerializedName("offset")
	private double[] offset;
	@SerializedName("rotation")
	private double[] rotation;

	/**
	 * Discard scale inherited from parent bone.
	 */
	public Boolean getIgnoreInheritedScale() {
		return ignoreInheritedScale;
	}

	public void setIgnoreInheritedScale(Boolean value) {
		this.ignoreInheritedScale = value;
	}

	/**
	 * Position of the locator in model space.
	 */
	public double[] getOffset() {
		return offset;
	}

	public void setOffset(double[] value) {
		this.offset = value;
	}

	/**
	 * Rotation of the locator in model space.
	 */
	public double[] getRotation() {
		return rotation;
	}

	public void setRotation(double[] value) {
		this.rotation = value;
	}
}
