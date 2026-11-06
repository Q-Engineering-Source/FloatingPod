package com.wi.floatingpod.bootstrap;

import java.util.Optional;

/**
 * 本发布版本支持的依赖身份。管理员不必填写内部 pin；实际发现仍须经过原 resolver 的完整校验。
 * 此清单来自已核验的发布输入，不根据本机发现结果重新签发信任。
 */
final class ManagedReleaseDependencies {
    static final String RELEASE = "FP_MANAGED_DEPENDENCIES_V1";

    private ManagedReleaseDependencies() { }

    static EarlyDependencyResolver.ExpectedPins expectedPins() {
        return new EarlyDependencyResolver.ExpectedPins(
            EarlyDependencyResolver.SQL_OUTER,
            Optional.of(new EarlyDependencyResolver.ScalarPins(
                "9FA05D84B72F31DF33C94BEB2B6C34BCA25A83FB5BD59B71AE1CF00678D563DC",
                "3.9.0",
                "B844BF6FB3080C73DF6B9C523B7A69B9EF4A980BB15E5C37C35443D1D0B0E0CD",
                "50F1B3DCF8F14975702DC6DA287797E391FC5D4834E4D3E7E9F9125EFDB2DE9A")),
            Optional.of(new EarlyDependencyResolver.FuguePins(
                "358E565B9B655840B52A79B7808EECD9C9B611030783180D68FA5BFF3B7CC615",
                "0.23.7")));
    }
}
