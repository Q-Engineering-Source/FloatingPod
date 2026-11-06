package com.wi.floatingpod.vendored.geckolib3.animation;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wi.floatingpod.vendored.geckolib3.core.builder.Animation;
import com.wi.floatingpod.vendored.geckolib3.core.builder.ILoopType.EDefaultLoopTypes;
import com.wi.floatingpod.vendored.geckolib3.core.easing.EasingType;
import com.wi.floatingpod.vendored.geckolib3.core.keyframe.BoneAnimation;
import com.wi.floatingpod.vendored.geckolib3.util.json.JsonAnimationUtils;
import com.wi.floatingpod.vendored.geckolib3.core.molang.MolangException;
import com.wi.floatingpod.vendored.geckolib3.core.molang.MolangParser;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 通过动画解析链验证正式无人机动画资产的运行时语义。
 */
class DroneAnimationAssetTest {
	/** 用于端到端动画解析的正式资源。 */
	private static final String ANIMATION_RESOURCE = "assets/floatingpod/animations/assembly/core/pod_core_v0_00_01.animation.json";

	/**
	 * 验证核心 off 与 idle 经 JsonAnimationUtils 解析为正确的循环、时长和缓动配置。
	 *
	 * @throws IOException 资源读取不应失败
	 */
	@Test
	void parsesDroneAnimationAssetWithLoopLengthAndEasing() throws IOException, MolangException {
		Map<String, Animation> animations = parseAnimations(readResource(ANIMATION_RESOURCE));

		assertEquals(2, animations.size());
		assertAnimation(animations.get("idle"), 40.0D, EDefaultLoopTypes.LOOP);
		assertAnimation(animations.get("off"), 20.0D, EDefaultLoopTypes.LOOP);

		BoneAnimation mainAnimation = findBoneAnimation(animations.get("idle"), "main");
		assertEquals(EasingType.EaseInOutQuad, mainAnimation.positionKeyFrames.yKeyFrames.get(1).easingType);
	}

	/**
	 * 使用生产动画转换链将资源根对象的每个动画转为 Animation。
	 *
	 * @param jsonAnimationText 动画资源文本
	 * @return 按资源名称索引的已解析动画
	 */
	private static Map<String, Animation> parseAnimations(String jsonAnimationText) throws MolangException {
		JsonObject root = JsonParser.parseString(jsonAnimationText).getAsJsonObject();
		MolangParser parser = new MolangParser();
		Map<String, Animation> animations = new LinkedHashMap<>();
		for (Map.Entry<String, JsonElement> entry : JsonAnimationUtils.getAnimations(root)) {
			animations.put(entry.getKey(), JsonAnimationUtils.deserializeJsonToAnimation(entry, parser));
		}
		return animations;
	}

	/**
	 * 断言一个动画保留其预期的 tick 时长和循环类型。
	 *
	 * @param animation 已解析动画
	 * @param expectedLength 预期 tick 时长
	 * @param expectedLoop 预期循环类型
	 */
	private static void assertAnimation(Animation animation, double expectedLength, EDefaultLoopTypes expectedLoop) {
		assertNotNull(animation);
		assertEquals(expectedLength, animation.animationLength);
		assertEquals(expectedLoop, animation.loop);
	}

	/**
	 * 从已解析动画中查找指定骨骼的关键帧集合。
	 *
	 * @param animation 已解析动画
	 * @param boneName 骨骼名称
	 * @return 对应骨骼动画
	 */
	private static BoneAnimation findBoneAnimation(Animation animation, String boneName) {
		for (BoneAnimation boneAnimation : animation.boneAnimations) {
			if (boneName.equals(boneAnimation.boneName)) {
				return boneAnimation;
			}
		}
		throw new AssertionError("Missing animation bone: " + boneName);
	}

	/**
	 * 从测试运行时 classpath 读取 UTF-8 资源。
	 *
	 * @param resourcePath 资源路径
	 * @return 资源内容
	 * @throws IOException 资源不可读时失败
	 */
	private static String readResource(String resourcePath) throws IOException {
		ClassLoader classLoader = DroneAnimationAssetTest.class.getClassLoader();
		try (InputStream inputStream = classLoader.getResourceAsStream(resourcePath)) {
			if (inputStream == null) {
				throw new IOException("Missing test resource: " + resourcePath);
			}
			return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
		}
	}
}
