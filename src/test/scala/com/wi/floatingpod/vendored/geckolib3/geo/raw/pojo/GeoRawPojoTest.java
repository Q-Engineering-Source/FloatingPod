package com.wi.floatingpod.vendored.geckolib3.geo.raw.pojo;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.wi.floatingpod.vendored.geckolib3.core.builder.Animation;
import com.wi.floatingpod.vendored.geckolib3.core.builder.ILoopType.EDefaultLoopTypes;
import com.wi.floatingpod.vendored.geckolib3.file.AnimationFile;
import com.wi.floatingpod.vendored.geckolib3.file.AnimationFileLoader;
import com.wi.floatingpod.vendored.geckolib3.file.GeoModelLoader;
import com.wi.floatingpod.vendored.geckolib3.geo.render.built.GeoBone;
import com.wi.floatingpod.vendored.geckolib3.geo.render.built.GeoModel;
import com.wi.floatingpod.vendored.geckolib3.core.molang.MolangException;
import com.wi.floatingpod.vendored.geckolib3.core.molang.MolangParser;
import net.minecraft.client.resources.IResource;
import net.minecraft.client.resources.IResourceManager;
import net.minecraft.client.resources.data.IMetadataSection;
import net.minecraft.util.ResourceLocation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 raw geo Gson 模型、联合适配器和真实无人机资产的行为。
 */
class GeoRawPojoTest {
	/** 用于端到端反序列化的正式 geo 资源。 */
	private static final String GEO_RESOURCE = "assets/floatingpod/geo/general_purpose_support_drone_001.geo.json";
	/** 用于端到端动画解析的正式动画资源。 */
	private static final String ANIMATION_RESOURCE =
			"assets/floatingpod/animations/general_purpose_support_drone_001.animation.json";

	/**
	 * 验证真实 geo 资源会保留骨骼、立方体和逐面 UV 数据。
	 *
	 * @throws IOException 资源读取或转换不应失败
	 */
	@Test
	void parsesDroneGeoAssetWithBonesCubesAndPerFaceUv() throws IOException {
		RawGeoModel rawGeoModel = Converter.fromJsonString(readResource(GEO_RESOURCE));

		assertEquals(FormatVersion.VERSION_1_12_0, rawGeoModel.getFormatVersion());
		assertNotNull(rawGeoModel.getMinecraftGeometry());
		assertEquals(1, rawGeoModel.getMinecraftGeometry().length);

		MinecraftGeometry geometry = rawGeoModel.getMinecraftGeometry()[0];
		assertNotNull(geometry.getBones());
		assertEquals(42, geometry.getBones().length);
		assertEquals(106, countCubes(geometry.getBones()));

		Cube firstCube = geometry.getBones()[0].getCubes()[0];
		assertNotNull(firstCube.getUv());
		assertFalse(firstCube.getUv().isBoxUV);
		assertNotNull(firstCube.getUv().faceUV);
		assertNotNull(firstCube.getUv().faceUV.getNorth());
		assertArrayEquals(new double[] { 0.0D, 13.0D }, firstCube.getUv().faceUV.getNorth().getUv());
		assertArrayEquals(new double[] { 6.0D, 6.0D }, firstCube.getUv().faceUV.getNorth().getUvSize());

		RawGeoModel roundTripped = Converter.fromJsonString(Converter.toJsonString(rawGeoModel));
		assertEquals(42, roundTripped.getMinecraftGeometry()[0].getBones().length);
		assertEquals(106, countCubes(roundTripped.getMinecraftGeometry()[0].getBones()));
	}

	/**
	 * 验证真实 geo 文件会经过资源加载、层级构造和 cube 构造的完整链路。
	 *
	 * @throws IOException 测试资源不可读时失败
	 */
	@Test
	void loadsDroneGeoAssetThroughGeoModelLoader() throws IOException {
		ResourceLocation location = new ResourceLocation(
				"floatingpod", "geo/general_purpose_support_drone_001.geo.json");
		GeoModel model = new GeoModelLoader().loadModel(resourceManagerFor(GEO_RESOURCE), location);

		assertEquals(42, countBuiltBones(model.topLevelBones));
		assertEquals(106, countBuiltCubes(model.topLevelBones));
		assertNotNull(model.getBone("main").orElseThrow());
	}

	/**
	 * 验证动画文件加载器保留四段动画、循环和保持最后一帧的语义。
	 *
	 * @throws IOException 测试资源不可读时失败
	 */
	@Test
	void loadsDroneAnimationsWithLoopSemantics() throws IOException, MolangException {
		ResourceLocation location = new ResourceLocation(
				"floatingpod", "animations/general_purpose_support_drone_001.animation.json");
		AnimationFile animationFile = new AnimationFileLoader().loadAllAnimations(
				new MolangParser(), location, resourceManagerFor(ANIMATION_RESOURCE));

		assertEquals(4, animationFile.getAllAnimations().size());
		assertAnimation(animationFile.getAnimation("idle"), EDefaultLoopTypes.LOOP, 40.0D);
		assertAnimation(animationFile.getAnimation("Weapon_on"), EDefaultLoopTypes.HOLD_ON_LAST_FRAME, 20.0D);
		assertAnimation(animationFile.getAnimation("Weapon_on_idle"), EDefaultLoopTypes.LOOP, 20.0D);
		assertAnimation(animationFile.getAnimation("Weapon_off"), EDefaultLoopTypes.HOLD_ON_LAST_FRAME, 20.0D);
	}

	/**
	 * 验证 UV 联合类型正确区分盒式数组和逐面对象，并拒绝非法输出状态。
	 */
	@Test
	void parsesAndSerializesUvUnionStrictly() {
		Gson gson = new Gson();
		UvUnion boxUv = gson.fromJson("[1,2]", UvUnion.class);
		UvUnion faceUv = gson.fromJson("{\"north\":{\"uv\":[3,4],\"uv_size\":[5,6]}}", UvUnion.class);

		assertTrue(boxUv.isBoxUV);
		assertArrayEquals(new double[] { 1.0D, 2.0D }, boxUv.boxUVCoords);
		assertFalse(faceUv.isBoxUV);
		assertArrayEquals(new double[] { 3.0D, 4.0D }, faceUv.faceUV.getNorth().getUv());
		assertArrayEquals(new double[] { 5.0D, 6.0D }, faceUv.faceUV.getNorth().getUvSize());
		assertEquals("[1.0,2.0]", gson.toJson(boxUv));

		assertUnionFailure(() -> gson.fromJson("null", UvUnion.class), "UvUnion");
		assertUnionFailure(() -> gson.fromJson("\"invalid\"", UvUnion.class), "UvUnion");
		assertUnionFailure(() -> gson.fromJson("[1,\"2\"]", UvUnion.class), "UvUnion");
		assertUnionFailure(() -> gson.toJson(new UvUnion()), "UvUnion");

		UvUnion ambiguous = new UvUnion();
		ambiguous.boxUVCoords = new double[] { 1.0D, 2.0D };
		ambiguous.faceUV = new UvFaces();
		assertUnionFailure(() -> gson.toJson(ambiguous), "UvUnion");
	}

	/**
	 * 验证多边形与定位器联合类型保留两个合法分支，并拒绝宽松的未知输入。
	 */
	@Test
	void parsesPolysAndLocatorUnionsStrictly() {
		Gson gson = new Gson();
		PolysUnion enumPolys = gson.fromJson("\"quad_list\"", PolysUnion.class);
		PolysUnion arrayPolys = gson.fromJson("[[[1,2,3]]]", PolysUnion.class);
		LocatorValue arrayLocator = gson.fromJson("[1,2,3]", LocatorValue.class);
		LocatorValue objectLocator = gson.fromJson("{\"offset\":[4,5,6]}", LocatorValue.class);

		assertEquals(PolysEnum.QUAD_LIST, enumPolys.enumValue);
		assertEquals(3.0D, arrayPolys.doubleArrayArrayArrayValue[0][0][2]);
		assertArrayEquals(new double[] { 1.0D, 2.0D, 3.0D }, arrayLocator.doubleArrayValue);
		assertArrayEquals(new double[] { 4.0D, 5.0D, 6.0D }, objectLocator.locatorClassValue.getOffset());
		assertEquals("\"quad_list\"", gson.toJson(enumPolys));
		assertEquals("[1.0,2.0,3.0]", gson.toJson(arrayLocator));

		assertUnionFailure(() -> gson.fromJson("\"quad\"", PolysUnion.class), "PolysUnion");
		assertUnionFailure(() -> gson.fromJson("[[1,2]]", PolysUnion.class), "PolysUnion");
		assertUnionFailure(() -> gson.fromJson("null", PolysUnion.class), "PolysUnion");
		assertUnionFailure(() -> gson.fromJson("null", LocatorValue.class), "LocatorValue");
		assertUnionFailure(() -> gson.fromJson("true", LocatorValue.class), "LocatorValue");
		assertUnionFailure(() -> gson.fromJson("[1,\"2\"]", LocatorValue.class), "LocatorValue");
		assertUnionFailure(() -> gson.toJson(new PolysUnion()), "PolysUnion");
		assertUnionFailure(() -> gson.toJson(new LocatorValue()), "LocatorValue");

		PolysUnion ambiguousPolys = new PolysUnion();
		ambiguousPolys.doubleArrayArrayArrayValue = new double[][][] { { { 1.0D } } };
		ambiguousPolys.enumValue = PolysEnum.TRI_LIST;
		assertUnionFailure(() -> gson.toJson(ambiguousPolys), "PolysUnion");

		LocatorValue ambiguousLocator = new LocatorValue();
		ambiguousLocator.doubleArrayValue = new double[] { 1.0D, 2.0D, 3.0D };
		ambiguousLocator.locatorClassValue = new LocatorClass();
		assertUnionFailure(() -> gson.toJson(ambiguousLocator), "LocatorValue");
	}

	/**
	 * 验证格式版本和多边形枚举仅接受规范中的值。
	 */
	@Test
	void rejectsUnknownFormatAndPolysValues() {
		Gson gson = new Gson();

		assertUnionFailure(() -> Converter.fromJsonString("{\"format_version\":\"1.16.0\",\"minecraft:geometry\":[]}"), "FormatVersion");
		assertUnionFailure(() -> gson.fromJson("\"triangle_list\"", PolysEnum.class), "PolysEnum");
		assertUnionFailure(() -> gson.fromJson("null", FormatVersion.class), "FormatVersion");
	}

	/**
	 * 从测试运行时 classpath 读取 UTF-8 资源。
	 *
	 * @param resourcePath 资源路径
	 * @return 资源内容
	 * @throws IOException 资源不可读时失败
	 */
	private static String readResource(String resourcePath) throws IOException {
		ClassLoader classLoader = GeoRawPojoTest.class.getClassLoader();
		try (InputStream inputStream = classLoader.getResourceAsStream(resourcePath)) {
			if (inputStream == null) {
				throw new IOException("Missing test resource: " + resourcePath);
			}
			return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	/**
	 * 为单个 classpath 资源提供直接调用加载器所需的资源管理器。
	 *
	 * @param expectedResource 唯一允许访问的资源路径
	 * @return 只暴露该资源的资源管理器
	 */
	private static IResourceManager resourceManagerFor(String expectedResource) {
		return new IResourceManager() {
			@Override
			public Set<String> getResourceDomains() {
				return Collections.singleton("floatingpod");
			}

			@Override
			public IResource getResource(ResourceLocation location) throws IOException {
				String resourcePath = "assets/" + location.getNamespace() + "/" + location.getPath();
				if (!expectedResource.equals(resourcePath)) {
					throw new IOException("Unexpected test resource: " + resourcePath);
				}
				byte[] content = readResource(resourcePath).getBytes(StandardCharsets.UTF_8);
				return new IResource() {
					@Override
					public ResourceLocation getResourceLocation() {
						return location;
					}

					@Override
					public InputStream getInputStream() {
						return new ByteArrayInputStream(content);
					}

					@Override
					public boolean hasMetadata() {
						return false;
					}

					@Override
					public <T extends IMetadataSection> T getMetadata(String sectionName) {
						return null;
					}

					@Override
					public String getResourcePackName() {
						return "floatingpod-test";
					}

					@Override
					public void close() {
					}
				};
			}

			@Override
			public List<IResource> getAllResources(ResourceLocation location) throws IOException {
				return Collections.singletonList(getResource(location));
			}
		};
	}

	/**
	 * 统计骨骼数组中实际声明的立方体总数。
	 *
	 * @param bones 资产中的骨骼
	 * @return 立方体总数
	 */
	private static int countCubes(Bone[] bones) {
		int cubeCount = 0;
		for (Bone bone : bones) {
			if (bone.getCubes() != null) {
				cubeCount += bone.getCubes().length;
			}
		}
		return cubeCount;
	}

	/**
	 * 递归统计已经由 GeoBuilder 构造的骨骼数量。
	 *
	 * @param bones 当前层的骨骼
	 * @return 当前层及其子树的骨骼数
	 */
	private static int countBuiltBones(List<GeoBone> bones) {
		int boneCount = 0;
		for (GeoBone bone : bones) {
			boneCount += 1 + countBuiltBones(bone.childBones);
		}
		return boneCount;
	}

	/**
	 * 递归统计已经由 GeoBuilder 构造的立方体数量。
	 *
	 * @param bones 当前层的骨骼
	 * @return 当前层及其子树的立方体数
	 */
	private static int countBuiltCubes(List<GeoBone> bones) {
		int cubeCount = 0;
		for (GeoBone bone : bones) {
			cubeCount += bone.childCubes.size() + countBuiltCubes(bone.childBones);
		}
		return cubeCount;
	}

	/**
	 * 断言解析动画的循环类型和转换为 tick 后的长度。
	 *
	 * @param animation 已解析的动画
	 * @param loop 预期循环类型
	 * @param length 预期 tick 长度
	 */
	private static void assertAnimation(Animation animation, EDefaultLoopTypes loop, double length) {
		assertNotNull(animation);
		assertEquals(loop, animation.loop);
		assertEquals(length, animation.animationLength);
	}

	/**
	 * 断言联合类型的失败消息仍标识其失败的目标类型。
	 *
	 * @param action 预期抛出解析异常的操作
	 * @param unionName 联合或枚举名称
	 */
	private static void assertUnionFailure(ThrowingAction action, String unionName) {
		JsonParseException exception = assertThrows(JsonParseException.class, action::run);
		assertTrue(exception.getMessage().contains(unionName));
	}

	/**
	 * 允许断言受检转换调用失败的函数接口。
	 */
	@FunctionalInterface
	private interface ThrowingAction {
		void run() throws Exception;
	}
}
