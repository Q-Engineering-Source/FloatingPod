/*
 * 改自 GeckoLib 3.0.31：迁移包和内部 import，把 MolangParser 指向 FloatingPod 独立实现，
 * 并将受检 MolangException 纳入资源加载失败边界。变更使动画资源加载链脱离外部 GeckoLib
 * 与 mclib，同时保留 Gson 文件解析流程和严格失败语义。
 */
package com.wi.floatingpod.vendored.geckolib3.file;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import com.wi.floatingpod.vendored.geckolib3.core.molang.MolangException;
import com.wi.floatingpod.vendored.geckolib3.core.molang.MolangParser;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import net.minecraft.client.resources.IResourceManager;
import net.minecraft.client.util.JsonException;
import net.minecraft.util.JsonUtils;
import net.minecraft.util.ResourceLocation;
import com.wi.floatingpod.vendored.geckolib3.GeckoLib;
import com.wi.floatingpod.vendored.geckolib3.core.builder.Animation;
import com.wi.floatingpod.vendored.geckolib3.util.json.JsonAnimationUtils;
public class AnimationFileLoader {
	public AnimationFile loadAllAnimations(MolangParser parser, ResourceLocation location, IResourceManager manager) {
		AnimationFile animationFile = new AnimationFile();
		JsonObject jsonRepresentation = loadFile(location, manager);
		Set<Map.Entry<String, JsonElement>> entrySet = JsonAnimationUtils.getAnimations(jsonRepresentation);
		for (Map.Entry<String, JsonElement> entry : entrySet) {
			String animationName = entry.getKey();
			Animation animation;
			try {
				animation = JsonAnimationUtils.deserializeJsonToAnimation(
						JsonAnimationUtils.getAnimation(jsonRepresentation, animationName), parser);
				animationFile.putAnimation(animationName, animation);
			} catch (JsonException | MolangException e) {
				GeckoLib.LOGGER.error("Could not load animation: {}", animationName, e);
				throw new RuntimeException(e);
			}
		}
		return animationFile;
	}
	/**
	 * Internal method for handling reloads of animation files. Do not override.
	 */
	private JsonObject loadFile(ResourceLocation location, IResourceManager manager) {
		String content = getResourceAsString(location, manager);
		Gson GSON = new Gson();
		return JsonUtils.fromJson(GSON, new StringReader(content), JsonObject.class);
	}
	public static String getResourceAsString(ResourceLocation location, IResourceManager manager) {
		try (InputStream inputStream = manager.getResource(location).getInputStream()) {
			return IOUtils.toString(inputStream, Charset.defaultCharset());
		} catch (Exception e) {
			String message = "Couldn't load " + location;
			GeckoLib.LOGGER.error(message, e);
			throw new RuntimeException(new FileNotFoundException(location.toString()));
		}
	}
}
