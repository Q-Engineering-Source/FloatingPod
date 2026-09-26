package com.wi.floatingpod.mixin;

import com.wi.floatingpod.client.render.BloomWorldPassHook;
import net.minecraft.client.renderer.EntityRenderer;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/**
 * 在半透明方块层与实体第二阶段之间提供中立的辉光消费边界。
 */
@Mixin(EntityRenderer.class)
public final class MixinEntityRenderer {

    /**
     * 将当前世界 pass 交给已安装的客户端辉光实现。
     *
     * @param pass vanilla 世界渲染 pass。
     * @param partialTicks 当前帧的插值比例。
     * @param finishTimeNano vanilla 世界渲染截止时间。
     * @param callbackInfo Mixin 注入回调信息。
     */
    @Inject(
            method = "renderWorldPass",
            at = @At(
                    value = "INVOKE",
                    target = "Lnet/minecraft/client/renderer/RenderGlobal;renderEntities(Lnet/minecraft/entity/Entity;Lnet/minecraft/client/renderer/culling/ICamera;F)V",
                    ordinal = 1,
                    shift = At.Shift.BEFORE
            )
    )
    private void dispatchBloomWorldPass(
            int pass,
            float partialTicks,
            long finishTimeNano,
            CallbackInfo callbackInfo) {
        BloomWorldPassHook.dispatch(pass, partialTicks);
    }
}
