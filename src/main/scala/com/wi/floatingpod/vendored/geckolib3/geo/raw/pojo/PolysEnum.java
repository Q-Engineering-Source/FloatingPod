/*
 * 改自 GeckoLib 3.0.31：为多边形枚举值接入 Gson TypeAdapter，并迁移到 FloatingPod 包名。
 * 非法枚举值会明确失败，保持多边形 JSON 的 Fail Fast 语义。
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
 * If not specifying vertex indices, arrays of data must be a list of tris or
 * quads, set by making this property either "tri_list" or "quad_list"
 */
@JsonAdapter(value = PolysEnum.Adapter.class, nullSafe = false)
public enum PolysEnum {
	QUAD_LIST,
	TRI_LIST;

	public String toValue() {
		switch (this) {
		case QUAD_LIST:
			return "quad_list";
		case TRI_LIST:
			return "tri_list";
		default:
			throw new IllegalStateException("Unsupported PolysEnum: " + this);
		}
	}

	public static PolysEnum forValue(String value) throws IOException {
		if ("quad_list".equals(value)) {
			return QUAD_LIST;
		}
		if ("tri_list".equals(value)) {
			return TRI_LIST;
		}
		throw new IOException("Cannot deserialize PolysEnum: " + value);
	}

	/**
	 * 强制多边形列表名称仅使用规范定义的两个值。
	 */
	public static final class Adapter extends TypeAdapter<PolysEnum> {
		@Override
		public PolysEnum read(JsonReader reader) throws IOException {
			if (reader.peek() != JsonToken.STRING) {
				throw new JsonParseException("Cannot deserialize PolysEnum from " + reader.peek());
			}
			try {
				return PolysEnum.forValue(reader.nextString());
			} catch (IOException exception) {
				throw new JsonParseException(exception.getMessage());
			}
		}

		@Override
		public void write(JsonWriter writer, PolysEnum value) throws IOException {
			if (value == null) {
				throw new JsonParseException("PolysEnum must not be null");
			}
			writer.value(value.toValue());
		}
	}
}
