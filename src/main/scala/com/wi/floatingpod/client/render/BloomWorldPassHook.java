package com.wi.floatingpod.client.render;

import net.minecraft.client.Minecraft;
import net.minecraft.client.renderer.RenderGlobal;
import net.minecraft.entity.Entity;

import java.util.Objects;

/**
 * 从 Mixin 取得当前客户端世界渲染参数并交给中立分派点。
 */
public final class BloomWorldPassHook {

    /**
     * 本类只提供静态入口，不应被实例化。
     */
    private BloomWorldPassHook() {
    }

    /**
     * 解析当前客户端世界渲染所需对象并分派辉光处理。
     *
     * @param renderPass vanilla 世界渲染 pass。
     * @param partialTicks 当前帧的插值比例。
     */
    public static void dispatch(int renderPass, float partialTicks) {
        Minecraft minecraft = Objects.requireNonNull(Minecraft.getMinecraft(), "minecraft");
        RenderGlobal renderGlobal = Objects.requireNonNull(minecraft.renderGlobal, "renderGlobal");
        Entity renderViewEntity = Objects.requireNonNull(
                minecraft.getRenderViewEntity(), "renderViewEntity");
        BloomWorldPass.dispatch(renderGlobal, renderViewEntity, partialTicks, renderPass);
    }
}
