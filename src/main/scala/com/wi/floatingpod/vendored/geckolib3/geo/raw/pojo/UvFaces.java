/*
 * 改自 GeckoLib 3.0.31：将逐面 UV 容器迁移到 FloatingPod 包名并改用 Gson 字段映射。
 * 这样各方向的 UV 对象可在无 Jackson 的模型加载链中保持原键名。
 */
package com.wi.floatingpod.vendored.geckolib3.geo.raw.pojo;

import com.google.gson.annotations.SerializedName;

/**
 * 立方体各方向的逐面 UV 定义。
 */
public class UvFaces {
	@SerializedName("down")
	private FaceUv down;
	@SerializedName("east")
	private FaceUv east;
	@SerializedName("north")
	private FaceUv north;
	@SerializedName("south")
	private FaceUv south;
	@SerializedName("up")
	private FaceUv up;
	@SerializedName("west")
	private FaceUv west;

	/**
	 * Specifies the UV's for the face that stretches along the x and z axes, and
	 * faces the -y axis
	 */
	public FaceUv getDown() {
		return down;
	}

	public void setDown(FaceUv value) {
		this.down = value;
	}

	/**
	 * Specifies the UV's for the face that stretches along the z and y axes, and
	 * faces the x axis
	 */
	public FaceUv getEast() {
		return east;
	}

	public void setEast(FaceUv value) {
		this.east = value;
	}

	/**
	 * Specifies the UV's for the face that stretches along the x and y axes, and
	 * faces the -z axis.
	 */
	public FaceUv getNorth() {
		return north;
	}

	public void setNorth(FaceUv value) {
		this.north = value;
	}

	/**
	 * Specifies the UV's for the face that stretches along the x and y axes, and
	 * faces the z axis
	 */
	public FaceUv getSouth() {
		return south;
	}

	public void setSouth(FaceUv value) {
		this.south = value;
	}

	/**
	 * Specifies the UV's for the face that stretches along the x and z axes, and
	 * faces the y axis
	 */
	public FaceUv getUp() {
		return up;
	}

	public void setUp(FaceUv value) {
		this.up = value;
	}

	/**
	 * Specifies the UV's for the face that stretches along the z and y axes, and
	 * faces the -x axis
	 */
	public FaceUv getWest() {
		return west;
	}

	public void setWest(FaceUv value) {
		this.west = value;
	}
}
