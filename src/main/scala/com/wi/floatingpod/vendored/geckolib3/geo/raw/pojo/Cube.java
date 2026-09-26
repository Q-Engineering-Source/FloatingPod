/*
 * 改自 GeckoLib 3.0.31：将 Cube 字段映射迁移为 Gson 注解并改用 FloatingPod 包名。
 * 这是移除 Jackson 依赖后保持几何 JSON 字段名称兼容所必需的调整。
 */
package com.wi.floatingpod.vendored.geckolib3.geo.raw.pojo;

import com.google.gson.annotations.SerializedName;

/**
 * 骨骼上的立方体几何定义。
 */
public class Cube {
	@SerializedName("inflate")
	private Double inflate;
	@SerializedName("mirror")
	private Boolean mirror;
	@SerializedName("origin")
	private double[] origin = new double[] { 0, 0, 0 };
	@SerializedName("pivot")
	private double[] pivot = new double[] { 0, 0, 0 };
	@SerializedName("rotation")
	private double[] rotation = new double[] { 0, 0, 0 };
	@SerializedName("size")
	private double[] size = new double[] { 1, 1, 1 };
	@SerializedName("uv")
	private UvUnion uv;

	/**
	 * Grow this box by this additive amount in all directions (in model space
	 * units), this field overrides the bone's inflate field for this cube only.
	 */
	public Double getInflate() {
		return inflate;
	}

	public void setInflate(Double value) {
		this.inflate = value;
	}

	/**
	 * Mirrors this cube about the unrotated x axis (effectively flipping the east /
	 * west faces), overriding the bone's 'mirror' setting for this cube.
	 */
	public Boolean getMirror() {
		return mirror;
	}

	public void setMirror(Boolean value) {
		this.mirror = value;
	}

	/**
	 * This point declares the unrotated lower corner of cube (smallest x/y/z value
	 * in model space units).
	 */
	public double[] getOrigin() {
		return origin;
	}

	public void setOrigin(double[] value) {
		this.origin = value;
	}

	/**
	 * If this field is specified, rotation of this cube occurs around this point,
	 * otherwise its rotation is around the center of the box.
	 */
	public double[] getPivot() {
		return pivot;
	}

	public void setPivot(double[] value) {
		this.pivot = value;
	}

	/**
	 * The cube is rotated by this amount (in degrees, x-then-y-then-z order) around
	 * the pivot.
	 */
	public double[] getRotation() {
		return rotation;
	}

	public void setRotation(double[] value) {
		this.rotation = value;
	}

	/**
	 * The cube extends this amount relative to its origin (in model space units).
	 */
	public double[] getSize() {
		return size;
	}

	public void setSize(double[] value) {
		this.size = value;
	}

	public UvUnion getUv() {
		return uv;
	}

	public void setUv(UvUnion value) {
		this.uv = value;
	}
}
