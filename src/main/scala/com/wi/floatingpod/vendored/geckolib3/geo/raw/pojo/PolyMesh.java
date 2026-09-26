/*
 * 改自 GeckoLib 3.0.31：将 PolyMesh 字段映射迁移为 Gson 注解并调整为 FloatingPod 包名。
 * 这保留实验性网格的数组字段，同时移除 Jackson 的反射入口。
 */
package com.wi.floatingpod.vendored.geckolib3.geo.raw.pojo;

import com.google.gson.annotations.SerializedName;

/**
 * ***EXPERIMENTAL*** A triangle or quad mesh object. Can be used in conjunction
 * with cubes and texture geometry.
 */
public class PolyMesh {
	@SerializedName("normalized_uvs")
	private Boolean normalizedUvs;
	@SerializedName("normals")
	private double[] normals;
	@SerializedName("polys")
	private PolysUnion polys;
	@SerializedName("positions")
	private double[] positions;
	@SerializedName("uvs")
	private double[] uvs;

	/**
	 * If true, UVs are assumed to be [0-1]. If false, UVs are assumed to be
	 * [0-texture_width] and [0-texture_height] respectively.
	 */
	public Boolean getNormalizedUvs() {
		return normalizedUvs;
	}

	public void setNormalizedUvs(Boolean value) {
		this.normalizedUvs = value;
	}

	/**
	 * Vertex normals. Can be either indexed via the "polys" section, or be a
	 * quad-list if mapped 1-to-1 to the positions and UVs sections.
	 */
	public double[] getNormals() {
		return normals;
	}

	public void setNormals(double[] value) {
		this.normals = value;
	}

	public PolysUnion getPolys() {
		return polys;
	}

	public void setPolys(PolysUnion value) {
		this.polys = value;
	}

	/**
	 * Vertex positions for the mesh. Can be either indexed via the "polys" section,
	 * or be a quad-list if mapped 1-to-1 to the normals and UVs sections.
	 */
	public double[] getPositions() {
		return positions;
	}

	public void setPositions(double[] value) {
		this.positions = value;
	}

	/**
	 * Vertex UVs. Can be either indexed via the "polys" section, or be a quad-list
	 * if mapped 1-to-1 to the positions and normals sections.
	 */
	public double[] getUvs() {
		return uvs;
	}

	public void setUvs(double[] value) {
		this.uvs = value;
	}
}
