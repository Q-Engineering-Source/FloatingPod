package com.wi.floatingpod;

/**
 * {@link SidedSetup} 的公共与专用服务端实现。
 *
 * <p>本类刻意不引用任何 {@code net.minecraft.client} 或 GeckoLib 渲染类型。
 * 专用服务端只会加载本实现，因此常量池中不出现客户端符号，
 * 这是门禁 3「服务端路径不得加载 Gecko 客户端类」在类加载层面的保证。
 *
 * <p>当前服务端没有本端独有的预初始化动作：实体注册属两端共用，在 mod 入口统一执行。
 * 这不是占位实现，而是该端确实无独有装配；后续服务端权威状态机落地时在此扩展。
 */
public class ServerSetupImpl implements SidedSetup {

    /**
     * 供 FML 的 {@code @SidedProxy} 反射实例化。
     */
    public ServerSetupImpl() {
    }

    /**
     * 执行服务端专属装配。当前无内容，原因见类注释。
     */
    @Override
    public void onPreInitialization() {
    }
}
