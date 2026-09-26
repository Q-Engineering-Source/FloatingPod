package com.wi.floatingpod.client.render;

import com.wi.floatingpod.entity.FloatingPodEntity;

import java.util.Objects;

/**
 * 一个实体在本帧的辉光绘制请求。
 *
 * <p>存在动机：辉光回调发生在实体渲染之后，此时 vanilla 传给 {@code doRender} 的相对偏移与
 * 插值偏航角都已不可得。若在回调时重新计算，插值比例可能与实体渲染那一刻不完全一致，
 * 导致辉光几何与伪发光几何错位。因此在实体渲染时就把当帧实际使用的定位量原样捕获下来，
 * 使两条路径必然对齐。
 *
 * <p>位置存为世界绝对坐标而非相对偏移：相对偏移的原点是摄像机，而摄像机位置在回调时才由
 * 辉光上下文给出，故只有绝对坐标是两个阶段都成立的表述。
 */
public final class BloomRequest {

    /** 待绘制实体。回调时需要它取得模型、贴图和颜色。 */
    private final FloatingPodEntity entity;

    /** 正常实体渲染已求得的不可变骨骼姿势。 */
    private final ModelPose pose;

    /** 实体插值后的世界 X 坐标。 */
    private final double worldX;

    /** 实体插值后的世界 Y 坐标。 */
    private final double worldY;

    /** 实体插值后的世界 Z 坐标。 */
    private final double worldZ;

    /** 实体渲染时 vanilla 给出的插值偏航角，原样保留以保证与伪发光批次同角度。 */
    private final float entityYaw;

    /** 实体渲染时的插值比例，回调时复用它取得渲染颜色。 */
    private final float partialTicks;

    /**
     * 捕获一次辉光绘制请求。
     *
     * @param entity       待绘制实体。
     * @param pose         正常实体渲染已求得的骨骼姿势。
     * @param worldX       实体插值后的世界 X 坐标。
     * @param worldY       实体插值后的世界 Y 坐标。
     * @param worldZ       实体插值后的世界 Z 坐标。
     * @param entityYaw    实体插值后的偏航角。
     * @param partialTicks 当前帧插值比例。
     */
    public BloomRequest(FloatingPodEntity entity, ModelPose pose, double worldX, double worldY, double worldZ,
                        float entityYaw, float partialTicks) {
        this.entity = Objects.requireNonNull(entity, "entity");
        this.pose = Objects.requireNonNull(pose, "pose");
        this.worldX = worldX;
        this.worldY = worldY;
        this.worldZ = worldZ;
        this.entityYaw = entityYaw;
        this.partialTicks = partialTicks;
    }

    /**
     * 返回待绘制实体。
     *
     * @return 实体实例。
     */
    public FloatingPodEntity entity() {
        return entity;
    }

    /**
     * 返回正常实体渲染已求得的骨骼姿势。
     *
     * @return 不可变模型姿势。
     */
    public ModelPose pose() {
        return pose;
    }

    /**
     * 返回实体插值后的世界 X 坐标。
     *
     * @return 世界 X 坐标。
     */
    public double worldX() {
        return worldX;
    }

    /**
     * 返回实体插值后的世界 Y 坐标。
     *
     * @return 世界 Y 坐标。
     */
    public double worldY() {
        return worldY;
    }

    /**
     * 返回实体插值后的世界 Z 坐标。
     *
     * @return 世界 Z 坐标。
     */
    public double worldZ() {
        return worldZ;
    }

    /**
     * 返回实体插值后的偏航角。
     *
     * @return 偏航角，单位为度。
     */
    public float entityYaw() {
        return entityYaw;
    }

    /**
     * 返回当前帧插值比例。
     *
     * @return 插值比例，取值 0 到 1。
     */
    public float partialTicks() {
        return partialTicks;
    }
}
