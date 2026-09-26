/*
 * 改自 GeckoLib 3.0.31：为格式版本联合表示接入 Gson TypeAdapter，并迁移包名。
 * 适配器同时接受数字和字符串版本，格式不匹配时立即抛错而不降级。
 */
package com.wi.floatingpod.vendored.geckolib3.geo.raw.pojo;

import java.io.IOException;

import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

/**
 * GeckoLib raw geo JSON 支持的格式版本。
 */
@JsonAdapter(value = FormatVersion.Adapter.class, nullSafe = false)
public enum FormatVersion {
	VERSION_1_12_0,
	VERSION_1_14_0;

	public String toValue() {
		switch (this) {
		case VERSION_1_12_0:
			return "1.12.0";
		case VERSION_1_14_0:
			return "1.14.0";
		default:
			throw new IllegalStateException("Unsupported FormatVersion: " + this);
		}
	}

	public static FormatVersion forValue(String value) throws IOException {
		if ("1.12.0".equals(value)) {
			return VERSION_1_12_0;
		}
		if ("1.14.0".equals(value)) {
			return VERSION_1_14_0;
		}
		throw new IOException("Cannot deserialize FormatVersion: " + value);
	}

	/**
	 * 强制格式版本仅能使用 GeckoLib 定义的 JSON 字符串。
	 */
	public static final class Adapter extends TypeAdapter<FormatVersion> {
		@Override
		public FormatVersion read(JsonReader reader) throws IOException {
			if (reader.peek() != JsonToken.STRING) {
				throw new JsonParseException("Cannot deserialize FormatVersion from " + reader.peek());
			}
			try {
				return FormatVersion.forValue(reader.nextString());
			} catch (IOException exception) {
				throw new JsonParseException(exception.getMessage());
			}
		}

		@Override
		public void write(JsonWriter writer, FormatVersion value) throws IOException {
			if (value == null) {
				throw new JsonParseException("FormatVersion must not be null");
			}
			writer.value(value.toValue());
		}
	}
}
