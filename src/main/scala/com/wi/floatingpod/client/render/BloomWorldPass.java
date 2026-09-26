package com.wi.floatingpod.client.render;

import net.minecraft.client.renderer.RenderGlobal;
import net.minecraft.entity.Entity;

import java.util.Objects;

/**
 * 向中立 Mixin 边界注册可选辉光实现的分派点。
 */
public final class BloomWorldPass {

    /** 可选的世界 pass 辉光处理器；Lumenized 未启用时保持为空。 */
    private static Fallback fallback;

    /**
     * 本类只提供静态分派，不应被实例化。
     */
    private BloomWorldPass() {
    }

    /**
     * 安装已完成注册的辉光处理器。
     *
     * @param candidate 要在世界 pass 边界执行的处理器。
     */
    public static void install(Fallback candidate) {
        fallback = Objects.requireNonNull(candidate, "candidate");
    }

    /**
     * 把一次世界 pass 交给可选的辉光处理器。
     *
     * @param renderGlobal 世界区块渲染器。
     * @param renderViewEntity 当前视角实体。
     * @param partialTicks 当前帧的插值比例。
     * @param renderPass vanilla 世界渲染 pass。
     */
    public static void dispatch(
            RenderGlobal renderGlobal,
            Entity renderViewEntity,
            float partialTicks,
            int renderPass) {
        Fallback installedFallback = fallback;
        if (installedFallback != null) {
            installedFallback.render(renderGlobal, renderViewEntity, partialTicks, renderPass);
        }
    }

    /**
     * 可选辉光实现在世界 pass 边界执行的操作。
     */
    @FunctionalInterface
    public interface Fallback {

        /**
         * 消费当前世界 pass 中待绘制的辉光请求。
         *
         * @param renderGlobal 世界区块渲染器。
         * @param renderViewEntity 当前视角实体。
         * @param partialTicks 当前帧的插值比例。
         * @param renderPass vanilla 世界渲染 pass。
         */
        void render(RenderGlobal renderGlobal, Entity renderViewEntity, float partialTicks, int renderPass);
    }
}
