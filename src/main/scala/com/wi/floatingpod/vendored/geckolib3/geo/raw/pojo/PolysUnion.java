/*
 * 改自 GeckoLib 3.0.31：为多边形数组/对象联合值实现 Gson TypeAdapter并迁移包名。
 * 数组和字典分支都保留且严格解析，避免把不支持的模型静默降为空数据。
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
 * 多边形列表枚举或三级 double 数组的严格联合类型。
 */
@JsonAdapter(value = PolysUnion.Adapter.class, nullSafe = false)
public class PolysUnion {
	public double[][][] doubleArrayArrayArrayValue;
	public PolysEnum enumValue;

	/**
	 * Gson 适配器将字符串限定为 PolysEnum，将数组限定为三级数值数组。
	 */
	public static final class Adapter extends TypeAdapter<PolysUnion> {
		private static final Gson GSON = new Gson();

		@Override
		public PolysUnion read(JsonReader reader) throws IOException {
			JsonElement element = JsonParser.parseReader(reader);
			if (element == null || element.isJsonNull()) {
				throw new JsonParseException("Cannot deserialize PolysUnion from null");
			}
			PolysUnion value = new PolysUnion();
			if (element.isJsonPrimitive() && element.getAsJsonPrimitive().isString()) {
				try {
					value.enumValue = PolysEnum.forValue(element.getAsString());
					return value;
				} catch (IOException exception) {
					throw new JsonParseException("Cannot deserialize PolysUnion enum: " + element.getAsString());
				}
			}
			if (element.isJsonArray()) {
				validateThreeDimensionalNumberArray(element.getAsJsonArray());
				value.doubleArrayArrayArrayValue = GSON.fromJson(element, double[][][].class);
				return value;
			}
			throw new JsonParseException("Cannot deserialize PolysUnion from " + element);
		}

		@Override
		public void write(JsonWriter writer, PolysUnion value) throws IOException {
			if (value == null) {
				throw new JsonParseException("PolysUnion must not be null");
			}
			boolean hasArray = value.doubleArrayArrayArrayValue != null;
			boolean hasEnum = value.enumValue != null;
			if (hasArray == hasEnum) {
				throw new JsonParseException("PolysUnion must contain exactly one value");
			}
			if (hasArray) {
				writer.jsonValue(GSON.toJson(value.doubleArrayArrayArrayValue));
				return;
			}
			writer.value(value.enumValue.toValue());
		}

		private static void validateThreeDimensionalNumberArray(JsonArray outerArray) {
			for (JsonElement middleElement : outerArray) {
				if (!middleElement.isJsonArray()) {
					throw new JsonParseException("PolysUnion array must be three-dimensional");
				}
				for (JsonElement innerElement : middleElement.getAsJsonArray()) {
					if (!innerElement.isJsonArray()) {
						throw new JsonParseException("PolysUnion array must be three-dimensional");
					}
					for (JsonElement numberElement : innerElement.getAsJsonArray()) {
						if (!numberElement.isJsonPrimitive() || !numberElement.getAsJsonPrimitive().isNumber()) {
							throw new JsonParseException("PolysUnion array must contain only JSON numbers");
						}
					}
				}
			}
		}
	}
}
