package com.wi.floatingpod.client.render;

import net.minecraft.client.renderer.OpenGlHelper;

/**
 * 伪发光光照作用域。
 *
 * <p>存在动机：{@code doc/01-technical-baseline.md} 要求以 {@code bloom} 开头的 bone
 * 在任何情况下都呈现「满亮度、不受世界光照影响」的伪发光效果。1.12.2 的实现手段是把光照贴图
 * 坐标钳制到满亮度的 240/240，绘制完成后还原为原值。
 *
 * <p>该坐标是 {@code draw()} 时刻的 OpenGL 状态而非逐顶点数据，因此必须在**绘制批次边界**
 * 进入与退出本作用域，不能在收集顶点的递归过程中切换；否则同一批次内的全部几何都会取同一亮度。
 *
 * <p>本类不吞掉异常也不构造降级路径：{@link #close()} 只负责还原它自己保存的状态。
 */
public final class EmissiveLightmapScope implements AutoCloseable {

    /** 1.12.2 光照贴图的满亮度坐标。240 对应光照等级 15，即不受世界光照衰减。 */
    private static final float FULL_BRIGHTNESS_COORDINATE = 240.0F;

    /** 进入作用域前的光照贴图 X 坐标，用于精确还原而非假定某个默认值。 */
    private final float previousBrightnessX;

    /** 进入作用域前的光照贴图 Y 坐标。 */
    private final float previousBrightnessY;

    /**
     * 保存当前光照贴图坐标并切换到满亮度。
     *
     * <p>构造即生效，配合 try-with-resources 保证无论绘制是否抛出都会还原状态，
     * 避免把满亮度泄漏给后续无关渲染。
     */
    public EmissiveLightmapScope() {
        this.previousBrightnessX = OpenGlHelper.lastBrightnessX;
        this.previousBrightnessY = OpenGlHelper.lastBrightnessY;
        OpenGlHelper.setLightmapTextureCoords(
                OpenGlHelper.lightmapTexUnit, FULL_BRIGHTNESS_COORDINATE, FULL_BRIGHTNESS_COORDINATE);
    }

    /**
     * 还原进入作用域前的光照贴图坐标。
     */
    @Override
    public void close() {
        OpenGlHelper.setLightmapTextureCoords(
                OpenGlHelper.lightmapTexUnit, previousBrightnessX, previousBrightnessY);
    }
}
