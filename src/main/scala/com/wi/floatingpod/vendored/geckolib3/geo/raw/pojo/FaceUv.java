/*
 * 改自 GeckoLib 3.0.31：将面 UV 数据类迁移到 FloatingPod 包名并采用 Gson 字段映射。
 * 该变更保留逐面 UV/尺寸字段，确保无 Jackson 时仍能读取模型纹理数据。
 */
package com.wi.floatingpod.vendored.geckolib3.geo.raw.pojo;

import com.google.gson.annotations.SerializedName;

/**
 * Specifies the UV's for the face that stretches along the x and z axes, and
 * faces the -y axis
 *
 * Specifies the UV's for the face that stretches along the z and y axes, and
 * faces the x axis
 *
 * Specifies the UV's for the face that stretches along the x and y axes, and
 * faces the -z axis.
 *
 * Specifies the UV's for the face that stretches along the x and y axes, and
 * faces the z axis
 *
 * Specifies the UV's for the face that stretches along the x and z axes, and
 * faces the y axis
 *
 * Specifies the UV's for the face that stretches along the z and y axes, and
 * faces the -x axis
 */
public class FaceUv {
	@SerializedName("material_instance")
	private String materialInstance;
	@SerializedName("uv")
	private double[] uv;
	@SerializedName("uv_size")
	private double[] uvSize;

	public String getMaterialInstance() {
		return materialInstance;
	}

	public void setMaterialInstance(String value) {
		this.materialInstance = value;
	}

	/**
	 * Specifies the uv origin for the face. For this face, it is the upper-left
	 * corner, when looking at the face with y being up.
	 */
	public double[] getUv() {
		return uv;
	}

	public void setUv(double[] value) {
		this.uv = value;
	}

	/**
	 * The face maps this many texels from the uv origin. If not specified, the box
	 * dimensions are used instead.
	 */
	public double[] getUvSize() {
		return uvSize;
	}

	public void setUvSize(double[] value) {
		this.uvSize = value;
	}
}
