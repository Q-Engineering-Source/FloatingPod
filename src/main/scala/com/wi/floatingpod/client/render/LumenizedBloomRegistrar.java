package com.wi.floatingpod.client.render;

import gregtech.client.utils.BloomEffectUtil;

/**
 * 辉光注册的隔离入口。
 *
 * <p>存在动机：客户端装配入口需要知道「辉光是否注册成功」，但不能因此认识任何 Lumenized 类型。
 * 若让它直接调用 {@link LumenizedBloomBridgeImpl#register()}，其常量池就会出现该方法返回类型
 * {@code BloomEffectUtil.BloomRenderTicket}，即 {@code gregtech/} 前缀符号；
 * 而装配入口在 Lumenized 缺失时仍必须能够加载。本类把 ticket 收敛为一个 {@code boolean}，
 * 使调用方的常量池保持干净。
 *
 * <p>本类与 {@link LumenizedBloomBridgeImpl} 是本项目仅有的两个引用 {@code gregtech} 包的类，
 * 二者都只在 {@code Loader.isModLoaded("lumenized")} 为真时才被触达，因此 Lumenized 缺失时
 * 不会被解析，也就不会抛 {@code NoClassDefFoundError}。
 */
public final class LumenizedBloomRegistrar {

    /**
     * 本类只提供静态入口，不应被实例化。
     */
    private LumenizedBloomRegistrar() {
    }

    /**
     * 创建辉光桥接并向 Lumenized 注册。
     *
     * <p>调用方必须已确认 Lumenized 已加载。本方法不重复该判定，
     * 因为判定必须发生在调用方一侧才能阻止本类被解析。
     *
     * <p>返回 {@code false} 表示 Lumenized 拒绝了注册。实测其 {@code registerBloomRender} 在
     * 检测到 Optifine shader pack 已加载时返回 {@code null}，此时辉光不会叠加，
     * 但伪发光基线不受影响。此处不抛异常也不吞掉该情形，而是原样如实返回，
     * 由调用方记录日志：这是上游明确的既定行为，不是本项目的错误状态。
     *
     * @param queue   与实体渲染器共享的辉光请求队列。
     * @param binding 与实体渲染器共享的绘制者绑定入口。
     * @return 注册成功返回 {@code true}；被 Lumenized 拒绝返回 {@code false}。
     */
    public static boolean register(BloomQueue queue, BloomPainterBinding binding) {
        LumenizedBloomBridgeImpl bridge = new LumenizedBloomBridgeImpl(queue, binding);
        BloomEffectUtil.BloomRenderTicket ticket = bridge.register();
        if (ticket == null) {
            return false;
        }
        BloomWorldPass.install(bridge);
        return true;
    }
}
