/*
 * 改自 GeckoLib 3.0.31：将模型属性字段迁移为 Gson 注解并放入 FloatingPod 命名空间。
 * 这样动画边界和渲染开关仍按原 JSON 键读取，不依赖 Jackson。
 */
package com.wi.floatingpod.vendored.geckolib3.geo.raw.pojo;

import com.google.gson.annotations.SerializedName;

/**
 * Geo 模型描述与可见边界属性。
 */
public class ModelProperties {
	@SerializedName("animationArmsDown")
	private Boolean animationArmsDown;
	@SerializedName("animationArmsOutFront")
	private Boolean animationArmsOutFront;
	@SerializedName("animationDontShowArmor")
	private Boolean animationDontShowArmor;
	@SerializedName("animationInvertedCrouch")
	private Boolean animationInvertedCrouch;
	@SerializedName("animationNoHeadBob")
	private Boolean animationNoHeadBob;
	@SerializedName("animationSingleArmAnimation")
	private Boolean animationSingleArmAnimation;
	@SerializedName("animationSingleLegAnimation")
	private Boolean animationSingleLegAnimation;
	@SerializedName("animationStationaryLegs")
	private Boolean animationStationaryLegs;
	@SerializedName("animationStatueOfLibertyArms")
	private Boolean animationStatueOfLibertyArms;
	@SerializedName("animationUpsideDown")
	private Boolean animationUpsideDown;
	@SerializedName("identifier")
	private String identifier;
	@SerializedName("preserve_model_pose")
	private Boolean preserveModelPose;
	@SerializedName("texture_height")
	private Double textureHeight;
	@SerializedName("texture_width")
	private Double textureWidth;
	@SerializedName("visible_bounds_height")
	private Double visibleBoundsHeight;
	@SerializedName("visible_bounds_offset")
	private double[] visibleBoundsOffset;
	@SerializedName("visible_bounds_width")
	private Double visibleBoundsWidth;

	public Boolean getAnimationArmsDown() {
		return animationArmsDown;
	}

	public void setAnimationArmsDown(Boolean value) {
		this.animationArmsDown = value;
	}

	public Boolean getAnimationArmsOutFront() {
		return animationArmsOutFront;
	}

	public void setAnimationArmsOutFront(Boolean value) {
		this.animationArmsOutFront = value;
	}

	public Boolean getAnimationDontShowArmor() {
		return animationDontShowArmor;
	}

	public void setAnimationDontShowArmor(Boolean value) {
		this.animationDontShowArmor = value;
	}

	public Boolean getAnimationInvertedCrouch() {
		return animationInvertedCrouch;
	}

	public void setAnimationInvertedCrouch(Boolean value) {
		this.animationInvertedCrouch = value;
	}

	public Boolean getAnimationNoHeadBob() {
		return animationNoHeadBob;
	}

	public void setAnimationNoHeadBob(Boolean value) {
		this.animationNoHeadBob = value;
	}

	public Boolean getAnimationSingleArmAnimation() {
		return animationSingleArmAnimation;
	}

	public void setAnimationSingleArmAnimation(Boolean value) {
		this.animationSingleArmAnimation = value;
	}

	public Boolean getAnimationSingleLegAnimation() {
		return animationSingleLegAnimation;
	}

	public void setAnimationSingleLegAnimation(Boolean value) {
		this.animationSingleLegAnimation = value;
	}

	public Boolean getAnimationStationaryLegs() {
		return animationStationaryLegs;
	}

	public void setAnimationStationaryLegs(Boolean value) {
		this.animationStationaryLegs = value;
	}

	public Boolean getAnimationStatueOfLibertyArms() {
		return animationStatueOfLibertyArms;
	}

	public void setAnimationStatueOfLibertyArms(Boolean value) {
		this.animationStatueOfLibertyArms = value;
	}

	public Boolean getAnimationUpsideDown() {
		return animationUpsideDown;
	}

	public void setAnimationUpsideDown(Boolean value) {
		this.animationUpsideDown = value;
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String value) {
		this.identifier = value;
	}

	public Boolean getPreserveModelPose() {
		return preserveModelPose;
	}

	public void setPreserveModelPose(Boolean value) {
		this.preserveModelPose = value;
	}

	/**
	 * Assumed height in texels of the texture that will be bound to this geometry.
	 */
	public Double getTextureHeight() {
		return textureHeight;
	}

	public void setTextureHeight(Double value) {
		this.textureHeight = value;
	}

	/**
	 * Assumed width in texels of the texture that will be bound to this geometry.
	 */
	public Double getTextureWidth() {
		return textureWidth;
	}

	public void setTextureWidth(Double value) {
		this.textureWidth = value;
	}

	/**
	 * Height of the visible bounding box (in model space units).
	 */
	public Double getVisibleBoundsHeight() {
		return visibleBoundsHeight;
	}

	public void setVisibleBoundsHeight(Double value) {
		this.visibleBoundsHeight = value;
	}

	/**
	 * Offset of the visibility bounding box from the entity location point (in
	 * model space units).
	 */
	public double[] getVisibleBoundsOffset() {
		return visibleBoundsOffset;
	}

	public void setVisibleBoundsOffset(double[] value) {
		this.visibleBoundsOffset = value;
	}

	/**
	 * Width of the visible bounding box (in model space units).
	 */
	public Double getVisibleBoundsWidth() {
		return visibleBoundsWidth;
	}

	public void setVisibleBoundsWidth(Double value) {
		this.visibleBoundsWidth = value;
	}
}
