package com.wi.floatingpod.bootstrap;

import com.wi.floatingpod.config.SourceLimitProfile;
import java.time.Duration;

/** 发布版本自带的普通配置模式预算；其身份写入安装记录，升级不能隐式改变已有安装。 */
public final class ManagedBootstrapPolicy {
    public static final String VERSION = "FP_MANAGED_EXISTING_DATABASE_POLICY_V1";
    public static final int ACTIVE_LIMIT = 256;
    private static final long MIB = 1024L * 1024L;
    private ManagedBootstrapPolicy() { }

    public static SourceLimitProfile sourceLimits() {
        // 这些是发布策略的明确上限，不是从当前数据库内容或历史部署草案推导的批准值。
        return SourceLimitProfile.of(64 * MIB, 8 * MIB, 64 * MIB, 64, 262144,
                512 * MIB, 16 * MIB, 16 * MIB);
    }

    public static DedicatedBootstrapBridge.Limits limits(long initializationMillis) {
        if (initializationMillis <= 0 || initializationMillis > 1_800_000)
            throw new IllegalArgumentException("Managed initialization deadline is outside the configuration bound");
        return new DedicatedBootstrapBridge.Limits(Duration.ofMillis(initializationMillis),
                Duration.ofSeconds(30), Duration.ofSeconds(60));
    }

    public static ProductionBootstrapControl.Policy policy(long initializationMillis) {
        return new ProductionBootstrapControl.Policy("NONE", "NONE", Duration.ofSeconds(20),
                Duration.ofSeconds(30), limits(initializationMillis));
    }

    public static Duration barrierWait() { return Duration.ofSeconds(10); }
}
