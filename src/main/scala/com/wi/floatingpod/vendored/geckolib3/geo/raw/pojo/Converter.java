/*
 * 改自 GeckoLib 3.0.31：用 Gson 转换入口及联合类型适配器替代 Jackson ObjectMapper。
 * 该改动同时保留日期处理和失败即抛出的行为，避免缺少 Jackson 时静默生成空模型。
 */
package com.wi.floatingpod.vendored.geckolib3.geo.raw.pojo;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;

/**
 * Geo JSON 与原始模型对象之间的严格转换入口。
 */
public final class Converter {
	private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
			.appendOptional(DateTimeFormatter.ISO_DATE_TIME).appendOptional(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
			.appendOptional(DateTimeFormatter.ISO_INSTANT)
			.appendOptional(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SX"))
			.appendOptional(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssX"))
			.appendOptional(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toFormatter().withZone(ZoneOffset.UTC);

	private static final DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder()
			.appendOptional(DateTimeFormatter.ISO_TIME).appendOptional(DateTimeFormatter.ISO_OFFSET_TIME)
			.parseDefaulting(ChronoField.YEAR, 2020).parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
			.parseDefaulting(ChronoField.DAY_OF_MONTH, 1).toFormatter().withZone(ZoneOffset.UTC);

	private static final Gson GSON = new GsonBuilder().create();

	private Converter() {
	}

	public static OffsetDateTime parseDateTimeString(String str) {
		return ZonedDateTime.from(DATE_TIME_FORMATTER.parse(str)).toOffsetDateTime();
	}

	public static OffsetTime parseTimeString(String str) {
		return ZonedDateTime.from(TIME_FORMATTER.parse(str)).toOffsetDateTime().toOffsetTime();
	}

	/**
	 * 将 geo JSON 解析为原始模型，格式或联合类型不匹配时立即失败。
	 *
	 * @param json geo JSON 文本
	 * @return 解析后的原始模型
	 * @throws IOException 保留上游调用面的受检异常签名
	 */
	public static RawGeoModel fromJsonString(String json) throws IOException {
		if (json == null) {
			throw new JsonParseException("RawGeoModel JSON must not be null");
		}
		return GSON.fromJson(json, RawGeoModel.class);
	}

	/**
	 * 将原始模型写为 geo JSON，非法联合类型会由其适配器拒绝。
	 *
	 * @param obj 原始模型
	 * @return JSON 文本
	 * @throws IOException 保留上游调用面的受检异常签名
	 */
	public static String toJsonString(RawGeoModel obj) throws IOException {
		if (obj == null) {
			throw new JsonParseException("RawGeoModel must not be null");
		}
		return GSON.toJson(obj);
	}
}
