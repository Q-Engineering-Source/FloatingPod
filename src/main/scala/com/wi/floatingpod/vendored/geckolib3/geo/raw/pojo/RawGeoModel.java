/*
 * 改自 GeckoLib 3.0.31：将顶层 raw geo 字段迁移为 Gson 注解并改用 FloatingPod 包名。
 * 这样 minecraft:geometry 根结构在无 Jackson 环境下仍保持严格映射。
 */
package com.wi.floatingpod.vendored.geckolib3.geo.raw.pojo;

import com.google.gson.annotations.SerializedName;

/**
 * 顶层 raw geo JSON 模型。
 */
public class RawGeoModel {
	@SerializedName("format_version")
	private FormatVersion formatVersion;
	@SerializedName("minecraft:geometry")
	private MinecraftGeometry[] minecraftGeometry;

	public FormatVersion getFormatVersion() {
		return formatVersion;
	}

	public void setFormatVersion(FormatVersion value) {
		this.formatVersion = value;
	}

	public MinecraftGeometry[] getMinecraftGeometry() {
		return minecraftGeometry;
	}

	public void setMinecraftGeometry(MinecraftGeometry[] value) {
		this.minecraftGeometry = value;
	}
}
