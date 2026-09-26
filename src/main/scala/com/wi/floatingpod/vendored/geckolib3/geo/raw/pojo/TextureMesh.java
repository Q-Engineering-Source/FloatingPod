/*
 * 改自 GeckoLib 3.0.31：将纹理网格字段迁移为 Gson 注解并调整包名。
 * 该改动保留纹理网格 JSON 键名，同时去除 Jackson 依赖。
 */
package com.wi.floatingpod.vendored.geckolib3.geo.raw.pojo;

import com.google.gson.annotations.SerializedName;

/**
 * 以纹理体素生成的实验性网格定义。
 */
public class TextureMesh {
	@SerializedName("local_pivot")
	private double[] localPivot;
	@SerializedName("position")
	private double[] position;
	@SerializedName("rotation")
	private double[] rotation;
	@SerializedName("scale")
	private double[] scale;
	@SerializedName("texture")
	private String texture;

	/**
	 * The pivot point on the texture (in *texture space* not entity or bone space)
	 * of the texture geometry
	 */
	public double[] getLocalPivot() {
		return localPivot;
	}

	public void setLocalPivot(double[] value) {
		this.localPivot = value;
	}

	/**
	 * The position of the pivot point after rotation (in *entity space* not texture
	 * or bone space) of the texture geometry
	 */
	public double[] getPosition() {
		return position;
	}

	public void setPosition(double[] value) {
		this.position = value;
	}

	/**
	 * The rotation (in degrees) of the texture geometry relative to the offset
	 */
	public double[] getRotation() {
		return rotation;
	}

	public void setRotation(double[] value) {
		this.rotation = value;
	}

	/**
	 * The scale (in degrees) of the texture geometry relative to the offset
	 */
	public double[] getScale() {
		return scale;
	}

	public void setScale(double[] value) {
		this.scale = value;
	}

	/**
	 * The friendly-named texture to use.
	 */
	public String getTexture() {
		return texture;
	}

	public void setTexture(String value) {
		this.texture = value;
	}
}
