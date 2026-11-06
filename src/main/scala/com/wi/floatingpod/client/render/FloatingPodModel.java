package com.wi.floatingpod.client.render;

import com.wi.floatingpod.FloatingPod;
import com.wi.floatingpod.entity.FloatingPodEntity;
import net.minecraft.util.ResourceLocation;
import com.wi.floatingpod.vendored.geckolib3.model.AnimatedGeoModel;

/**
 * 浮游辅助单元的 GeckoLib 模型提供器。
 *
 * <p>本类位于客户端侧：其基类 {@link AnimatedGeoModel} 引用 {@code net.minecraft.client}
 * 与 {@code GeckoLibCache}，只能在客户端加载。它负责把实体映射到模型、贴图与动画三个资源路径。
 *
 * <p>三个目录名不可自行改动：{@code geo} 与 {@code animations} 由 GeckoLib 的资源扫描逻辑
 * 硬编码枚举，改名会导致资产不被加载。
 */
public class FloatingPodModel extends AnimatedGeoModel<FloatingPodEntity> {

    /** 模型几何文件位置，对应 Blockbench 导出的 {@code .geo.json}。 */
    private static final ResourceLocation MODEL_LOCATION =
            new ResourceLocation(FloatingPod.MOD_ID, "geo/assembly/core/pod_core_v0_00_01.geo.json");

    /** 贴图位置。贴图为二进制资源，已由 {@code .gitattributes} 标记为 {@code -text}。 */
    private static final ResourceLocation TEXTURE_LOCATION =
            new ResourceLocation(FloatingPod.MOD_ID, "textures/entity/assembly/core/pod_core_v0_00_01.png");

    /** 动画文件位置，其内动画键必须与实体侧使用的动画名一致。 */
    private static final ResourceLocation ANIMATION_LOCATION =
            new ResourceLocation(FloatingPod.MOD_ID, "animations/assembly/core/pod_core_v0_00_01.animation.json");

    /**
     * 提供模型几何资源位置。
     *
     * @param entity 待渲染实体；当前所有实例共用同一模型。
     * @return 模型资源位置。
     */
    @Override
    public ResourceLocation getModelLocation(FloatingPodEntity entity) {
        return MODEL_LOCATION;
    }

    /**
     * 提供贴图资源位置。
     *
     * @param entity 待渲染实体；当前所有实例共用同一贴图。
     * @return 贴图资源位置。
     */
    @Override
    public ResourceLocation getTextureLocation(FloatingPodEntity entity) {
        return TEXTURE_LOCATION;
    }

    /**
     * 提供动画资源位置。
     *
     * @param entity 待渲染实体；当前所有实例共用同一动画文件。
     * @return 动画资源位置。
     */
    @Override
    public ResourceLocation getAnimationFileLocation(FloatingPodEntity entity) {
        return ANIMATION_LOCATION;
    }
}
