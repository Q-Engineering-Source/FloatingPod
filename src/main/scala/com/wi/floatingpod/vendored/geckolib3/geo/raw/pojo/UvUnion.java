/*
 * 改自 GeckoLib 3.0.31：为 UV 数组/逐面字典联合值实现 Gson TypeAdapter并迁移包名。
 * 两个 JSON 分支均严格保留，确保每个面的 uv 与 uv_size 都能被正确读取。
 */
package com.wi.floatingpod.vendored.geckolib3.geo.raw.pojo;

import java.io.IOException;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * 立方体盒式 UV 坐标或逐面 UV 对象的严格联合类型。
 */
@JsonAdapter(value = UvUnion.Adapter.class, nullSafe = false)
public class UvUnion {
	public double[] boxUVCoords;
	public UvFaces faceUV;
	public boolean isBoxUV;

	/**
	 * Gson 适配器根据 JSON 容器类型选择唯一的联合分支。
	 */
	public static final class Adapter extends TypeAdapter<UvUnion> {
		private static final Gson GSON = new Gson();

		@Override
		public UvUnion read(JsonReader reader) throws IOException {
			JsonElement element = JsonParser.parseReader(reader);
			if (element == null || element.isJsonNull()) {
				throw new JsonParseException("Cannot deserialize UvUnion from null");
			}
			UvUnion value = new UvUnion();
			if (element.isJsonArray()) {
				validateNumberArray(element.getAsJsonArray(), "UvUnion.boxUVCoords");
				value.boxUVCoords = GSON.fromJson(element, double[].class);
				value.isBoxUV = true;
				return value;
			}
			if (element.isJsonObject()) {
				value.faceUV = GSON.fromJson(element, UvFaces.class);
				if (value.faceUV == null) {
					throw new JsonParseException("Cannot deserialize UvUnion.faceUV");
				}
				value.isBoxUV = false;
				return value;
			}
			throw new JsonParseException("Cannot deserialize UvUnion from " + element);
		}

		@Override
		public void write(JsonWriter writer, UvUnion value) throws IOException {
			if (value == null) {
				throw new JsonParseException("UvUnion must not be null");
			}
			boolean hasBox = value.boxUVCoords != null;
			boolean hasFaces = value.faceUV != null;
			if (hasBox == hasFaces) {
				throw new JsonParseException("UvUnion must contain exactly one value");
			}
			if (hasBox) {
				writer.jsonValue(GSON.toJson(value.boxUVCoords));
				return;
			}
			writer.jsonValue(GSON.toJson(value.faceUV));
		}

		private static void validateNumberArray(JsonArray array, String branch) {
			for (JsonElement element : array) {
				if (!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isNumber()) {
					throw new JsonParseException(branch + " must contain only JSON numbers");
				}
			}
		}
	}
}
