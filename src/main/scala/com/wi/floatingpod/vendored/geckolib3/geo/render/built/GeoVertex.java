/*
 * 改自 GeckoLib 3.0.31：迁移 GeoVertex 包名并保留 vecmath 与 commons 数组校验 import。
 * 这样顶点校验和坐标表示继续由本地 geo 渲染闭包提供。
 */
package com.wi.floatingpod.vendored.geckolib3.geo.render.built;
import javax.vecmath.Vector3f;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.Validate;
public class GeoVertex {
	public final Vector3f position;
	public float textureU;
	public float textureV;
	public GeoVertex(float x, float y, float z) {
		this.position = new Vector3f(x, y, z);
	}
	public GeoVertex(double x, double y, double z) {
		this.position = new Vector3f((float) x, (float) y, (float) z);
	}
	public GeoVertex setTextureUV(float texU, float texV) {
		return new GeoVertex(this.position, texU, texV);
	}
	public GeoVertex setTextureUV(double[] array) {
		Validate.validIndex(ArrayUtils.toObject(array), 1);
		return new GeoVertex(this.position, (float) array[0], (float) array[1]);
	}
	public GeoVertex(Vector3f posIn, float texU, float texV) {
		this.position = posIn;
		this.textureU = texU;
		this.textureV = texV;
	}
}
