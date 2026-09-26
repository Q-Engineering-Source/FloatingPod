/*
 * 改自 GeckoLib 3.0.31：为定位器数组/对象联合值实现 Gson TypeAdapter，并迁移包名。
 * 两种 JSON 分支均严格解析，避免 Jackson 缺失时丢失定位器数据。
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
 * 定位器坐标数组或定位器对象的严格联合类型。
 */
@JsonAdapter(value = LocatorValue.Adapter.class, nullSafe = false)
public class LocatorValue {
	public LocatorClass locatorClassValue;
	public double[] doubleArrayValue;

	/**
	 * Gson 适配器根据 JSON 容器类型选择唯一的定位器表示。
	 */
	public static final class Adapter extends TypeAdapter<LocatorValue> {
		private static final Gson GSON = new Gson();

		@Override
		public LocatorValue read(JsonReader reader) throws IOException {
			JsonElement element = JsonParser.parseReader(reader);
			if (element == null || element.isJsonNull()) {
				throw new JsonParseException("Cannot deserialize LocatorValue from null");
			}
			LocatorValue value = new LocatorValue();
			if (element.isJsonArray()) {
				validateNumberArray(element.getAsJsonArray(), "LocatorValue.doubleArrayValue");
				value.doubleArrayValue = GSON.fromJson(element, double[].class);
				return value;
			}
			if (element.isJsonObject()) {
				value.locatorClassValue = GSON.fromJson(element, LocatorClass.class);
				if (value.locatorClassValue == null) {
					throw new JsonParseException("Cannot deserialize LocatorValue.locatorClassValue");
				}
				return value;
			}
			throw new JsonParseException("Cannot deserialize LocatorValue from " + element);
		}

		@Override
		public void write(JsonWriter writer, LocatorValue value) throws IOException {
			if (value == null) {
				throw new JsonParseException("LocatorValue must not be null");
			}
			boolean hasClass = value.locatorClassValue != null;
			boolean hasArray = value.doubleArrayValue != null;
			if (hasClass == hasArray) {
				throw new JsonParseException("LocatorValue must contain exactly one value");
			}
			if (hasClass) {
				writer.jsonValue(GSON.toJson(value.locatorClassValue));
				return;
			}
			writer.jsonValue(GSON.toJson(value.doubleArrayValue));
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
