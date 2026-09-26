/*
 * 改自 GeckoLib 3.0.31：将上游 molang 路径映射到 FloatingPod 的 core/molang，并删除冗余 import。
 * 该例外路径映射与本地 MolangParser/Variable 配套，避免重新引入外部 mclib 命名空间。
 */
package com.wi.floatingpod.vendored.geckolib3.core.molang;

import com.wi.floatingpod.vendored.mclib.math.Variable;

public class MolangRegistrar {
	public static void registerVars(MolangParser parser) {
		parser.register(new Variable("query.anim_time", 0));
		parser.register(new Variable("query.actor_count", 0));
		parser.register(new Variable("query.health", 0));
		parser.register(new Variable("query.max_health", 0));
		parser.register(new Variable("query.distance_from_camera", 0));
		parser.register(new Variable("query.yaw_speed", 0));
		parser.register(new Variable("query.is_in_water_or_rain", 0));
		parser.register(new Variable("query.is_in_water", 0));
		parser.register(new Variable("query.is_on_ground", 0));
		parser.register(new Variable("query.time_of_day", 0));
		parser.register(new Variable("query.is_on_fire", 0));
		parser.register(new Variable("query.ground_speed", 0));
	}
}
