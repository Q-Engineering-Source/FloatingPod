/*
 * 改自 GeckoLib 3.0.31：迁移 IGeoBuilder 接口及其 geo 类型 import 到 FloatingPod 命名空间。
 * 这样构建器契约保持可替换，同时不暴露外部 GeckoLib 类。
 */
package com.wi.floatingpod.vendored.geckolib3.geo.render;
import com.wi.floatingpod.vendored.geckolib3.geo.raw.pojo.ModelProperties;
import com.wi.floatingpod.vendored.geckolib3.geo.raw.tree.RawBoneGroup;
import com.wi.floatingpod.vendored.geckolib3.geo.raw.tree.RawGeometryTree;
import com.wi.floatingpod.vendored.geckolib3.geo.render.built.GeoBone;
import com.wi.floatingpod.vendored.geckolib3.geo.render.built.GeoModel;
public interface IGeoBuilder {
	GeoModel constructGeoModel(RawGeometryTree geometryTree);
	GeoBone constructBone(RawBoneGroup bone, ModelProperties properties, GeoBone parent);
}
