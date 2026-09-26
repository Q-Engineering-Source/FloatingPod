/*
 * 改自 GeckoLib 3.0.31：迁移包和 geo 构建链 import，并接入 FloatingPod 的 Gson Converter，
 * 同时适配 Cleanroom ResourceLocation 的 namespace API。这样模型加载保持严格失败语义，
 * 且不需要 Jackson、外部 GeckoLib 类或已不存在的 getResourceDomain 调用。
 */
package com.wi.floatingpod.vendored.geckolib3.file;
import net.minecraft.client.resources.IResourceManager;
import net.minecraft.util.ResourceLocation;
import com.wi.floatingpod.vendored.geckolib3.GeckoLib;
import com.wi.floatingpod.vendored.geckolib3.geo.exception.GeoModelException;
import com.wi.floatingpod.vendored.geckolib3.geo.raw.pojo.Converter;
import com.wi.floatingpod.vendored.geckolib3.geo.raw.pojo.FormatVersion;
import com.wi.floatingpod.vendored.geckolib3.geo.raw.pojo.RawGeoModel;
import com.wi.floatingpod.vendored.geckolib3.geo.raw.tree.RawGeometryTree;
import com.wi.floatingpod.vendored.geckolib3.geo.render.GeoBuilder;
import com.wi.floatingpod.vendored.geckolib3.geo.render.built.GeoModel;
public class GeoModelLoader {
	public GeoModel loadModel(IResourceManager resourceManager, ResourceLocation location) {
		try {
			// Deserialize from json into basic json objects, bones are still stored as a
			// flat list
			RawGeoModel rawModel = Converter
					.fromJsonString(AnimationFileLoader.getResourceAsString(location, resourceManager));
			if (rawModel.getFormatVersion() != FormatVersion.VERSION_1_12_0) {
				throw new GeoModelException(location, "Wrong geometry json version, expected 1.12.0");
			}
			// Parse the flat list of bones into a raw hierarchical tree of "BoneGroup"s
			RawGeometryTree rawGeometryTree = RawGeometryTree.parseHierarchy(rawModel, location);
			// Build the quads and cubes from the raw tree into a built and ready to be
			// rendered GeoModel
			return GeoBuilder.getGeoBuilder(location.getNamespace()).constructGeoModel(rawGeometryTree);
		} catch (Exception e) {
			GeckoLib.LOGGER.error(String.format("Error parsing %S", location), e);
			throw (new RuntimeException(e));
		}
	}
}
