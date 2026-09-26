/*
 * 改自 GeckoLib 3.0.31：将 Jackson 字段映射改为 Gson 注解并迁移到 FloatingPod 包名。
 * 这样骨骼 JSON 可在无 Jackson 运行库的客户端中严格反序列化。
 */
package com.wi.floatingpod.vendored.geckolib3.geo.raw.pojo;

import java.util.Map;

import com.google.gson.annotations.SerializedName;

/**
 * Geo 模型中可动画的骨骼。
 */
public class Bone {
	@SerializedName("bind_pose_rotation")
	private double[] bindPoseRotation;
	@SerializedName("cubes")
	private Cube[] cubes;
	@SerializedName("debug")
	private Boolean debug;
	@SerializedName("inflate")
	private Double inflate;
	@SerializedName("locators")
	private Map<String, LocatorValue> locators;
	@SerializedName("mirror")
	private Boolean mirror;
	@SerializedName("name")
	private String name;
	@SerializedName("neverRender")
	private Boolean neverRender;
	@SerializedName("parent")
	private String parent;
	@SerializedName("pivot")
	private double[] pivot = new double[] { 0, 0, 0 };
	@SerializedName("poly_mesh")
	private PolyMesh polyMesh;
	@SerializedName("render_group_id")
	private Long renderGroupID;
	@SerializedName("reset")
	private Boolean reset;
	@SerializedName("rotation")
	private double[] rotation = new double[] { 0, 0, 0 };
	@SerializedName("texture_meshes")
	private TextureMesh[] textureMeshes;

	public double[] getBindPoseRotation() {
		return bindPoseRotation;
	}

	public void setBindPoseRotation(double[] value) {
		this.bindPoseRotation = value;
	}

	/**
	 * This is the list of cubes associated with this bone.
	 */
	public Cube[] getCubes() {
		return cubes;
	}

	public void setCubes(Cube[] value) {
		this.cubes = value;
	}

	public Boolean getDebug() {
		return debug;
	}

	public void setDebug(Boolean value) {
		this.debug = value;
	}

	/**
	 * Grow this box by this additive amount in all directions (in model space
	 * units).
	 */
	public Double getInflate() {
		return inflate;
	}

	public void setInflate(Double value) {
		this.inflate = value;
	}

	/**
	 * This is a list of locators associated with this bone. A locator is a point in
	 * model space that tracks a particular bone as the bone animates (by
	 * maintaining it's relationship to the bone through the animation).
	 */
	public Map<String, LocatorValue> getLocators() {
		return locators;
	}

	public void setLocators(Map<String, LocatorValue> value) {
		this.locators = value;
	}

	/**
	 * Mirrors the UV's of the unrotated cubes along the x axis, also causes the
	 * east/west faces to get flipped.
	 */
	public Boolean getMirror() {
		return mirror;
	}

	public void setMirror(Boolean value) {
		this.mirror = value;
	}

	/**
	 * Animation files refer to this bone via this identifier.
	 */
	public String getName() {
		return name;
	}

	public void setName(String value) {
		this.name = value;
	}

	public Boolean getNeverRender() {
		return neverRender;
	}

	public void setNeverRender(Boolean value) {
		this.neverRender = value;
	}

	/**
	 * Bone that this bone is relative to. If the parent bone moves, this bone will
	 * move along with it.
	 */
	public String getParent() {
		return parent;
	}

	public void setParent(String value) {
		this.parent = value;
	}

	/**
	 * The bone pivots around this point (in model space units).
	 */
	public double[] getPivot() {
		return pivot;
	}

	public void setPivot(double[] value) {
		this.pivot = value;
	}

	/**
	 * ***EXPERIMENTAL*** A triangle or quad mesh object. Can be used in conjunction
	 * with cubes and texture geometry.
	 */
	public PolyMesh getPolyMesh() {
		return polyMesh;
	}

	public void setPolyMesh(PolyMesh value) {
		this.polyMesh = value;
	}

	public Long getRenderGroupID() {
		return renderGroupID;
	}

	public void setRenderGroupID(Long value) {
		this.renderGroupID = value;
	}

	public Boolean getReset() {
		return reset;
	}

	public void setReset(Boolean value) {
		this.reset = value;
	}

	/**
	 * This is the initial rotation of the bone around the pivot, pre-animation (in
	 * degrees, x-then-y-then-z order).
	 */
	public double[] getRotation() {
		return rotation;
	}

	public void setRotation(double[] value) {
		this.rotation = value;
	}

	/**
	 * ***EXPERIMENTAL*** Adds a mesh to the bone's geometry by converting texels in
	 * a texture into boxes.
	 */
	public TextureMesh[] getTextureMeshes() {
		return textureMeshes;
	}

	public void setTextureMeshes(TextureMesh[] value) {
		this.textureMeshes = value;
	}
}
