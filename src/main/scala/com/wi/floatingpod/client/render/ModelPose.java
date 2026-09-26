package com.wi.floatingpod.client.render;

import com.wi.floatingpod.vendored.geckolib3.core.processor.IBone;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 一次实体动画求值后，共享模型全部骨骼的不可变变换快照。
 *
 * <p>Gecko 模型会在同类型实体之间共享骨骼实例，而 Lumenized 在正常实体渲染结束后才消费辉光请求。
 * 因此辉光路径不能再次推进动画，也不能依赖共享骨骼当时残留的姿势；它必须回放实体正常渲染时
 * 已经求得的姿势。本类型只保存动画会修改的旋转、位置和缩放，不保存任何可变骨骼引用。
 */
public final class ModelPose {

    /** 按动画处理器登记顺序保存的全部骨骼变换。 */
    private final List<BoneTransform> transforms;

    /**
     * 创建不可变模型姿势。
     *
     * @param transforms 已复制的骨骼变换
     */
    private ModelPose(List<BoneTransform> transforms) {
        this.transforms = Collections.unmodifiableList(new ArrayList<>(transforms));
    }

    /**
     * 捕获当前共享骨骼的完整变换状态。
     *
     * @param bones 动画处理器登记的共享骨骼
     * @return 与当前骨骼顺序和名称绑定的不可变姿势
     */
    public static ModelPose capture(List<? extends IBone> bones) {
        Objects.requireNonNull(bones, "bones");
        List<BoneTransform> transforms = new ArrayList<>(bones.size());
        for (IBone bone : bones) {
            transforms.add(BoneTransform.capture(Objects.requireNonNull(bone, "bone")));
        }
        return new ModelPose(transforms);
    }

    /**
     * 将已捕获姿势应用到当前共享骨骼。
     *
     * <p>骨骼数量、顺序或名称变化表示请求捕获后模型布局已变化。此时继续绘制会把姿势写入错误骨骼，
     * 因此立即失败，而不是尝试猜测兼容关系。
     *
     * @param bones 当前动画处理器登记的共享骨骼
     */
    public void apply(List<? extends IBone> bones) {
        Objects.requireNonNull(bones, "bones");
        if (bones.size() != transforms.size()) {
            throw new IllegalStateException(
                    "模型姿势骨骼数量不匹配: expected=" + transforms.size() + ", actual=" + bones.size());
        }
        for (int index = 0; index < transforms.size(); index++) {
            IBone bone = Objects.requireNonNull(bones.get(index), "bone");
            transforms.get(index).applyTo(bone, index);
        }
    }

    /** 单个骨骼的不可变九分量变换。 */
    private static final class BoneTransform {

        /** 骨骼名称，用于应用时校验模型布局。 */
        private final String name;

        /** X 轴旋转。 */
        private final float rotationX;

        /** Y 轴旋转。 */
        private final float rotationY;

        /** Z 轴旋转。 */
        private final float rotationZ;

        /** X 轴位置。 */
        private final float positionX;

        /** Y 轴位置。 */
        private final float positionY;

        /** Z 轴位置。 */
        private final float positionZ;

        /** X 轴缩放。 */
        private final float scaleX;

        /** Y 轴缩放。 */
        private final float scaleY;

        /** Z 轴缩放。 */
        private final float scaleZ;

        /**
         * 创建单个骨骼变换。
         *
         * @param name 骨骼名称
         * @param rotationX X 轴旋转
         * @param rotationY Y 轴旋转
         * @param rotationZ Z 轴旋转
         * @param positionX X 轴位置
         * @param positionY Y 轴位置
         * @param positionZ Z 轴位置
         * @param scaleX X 轴缩放
         * @param scaleY Y 轴缩放
         * @param scaleZ Z 轴缩放
         */
        private BoneTransform(String name, float rotationX, float rotationY, float rotationZ,
                              float positionX, float positionY, float positionZ,
                              float scaleX, float scaleY, float scaleZ) {
            this.name = name;
            this.rotationX = rotationX;
            this.rotationY = rotationY;
            this.rotationZ = rotationZ;
            this.positionX = positionX;
            this.positionY = positionY;
            this.positionZ = positionZ;
            this.scaleX = scaleX;
            this.scaleY = scaleY;
            this.scaleZ = scaleZ;
        }

        /**
         * 捕获单个骨骼的变换。
         *
         * @param bone 待捕获骨骼
         * @return 不可变变换
         */
        private static BoneTransform capture(IBone bone) {
            return new BoneTransform(Objects.requireNonNull(bone.getName(), "bone.name"),
                    bone.getRotationX(), bone.getRotationY(), bone.getRotationZ(),
                    bone.getPositionX(), bone.getPositionY(), bone.getPositionZ(),
                    bone.getScaleX(), bone.getScaleY(), bone.getScaleZ());
        }

        /**
         * 校验目标骨骼身份并写入全部变换分量。
         *
         * @param bone 目标共享骨骼
         * @param index 骨骼登记序号
         */
        private void applyTo(IBone bone, int index) {
            if (!name.equals(bone.getName())) {
                throw new IllegalStateException("模型姿势骨骼不匹配: index=" + index
                        + ", expected=" + name + ", actual=" + bone.getName());
            }
            bone.setRotationX(rotationX);
            bone.setRotationY(rotationY);
            bone.setRotationZ(rotationZ);
            bone.setPositionX(positionX);
            bone.setPositionY(positionY);
            bone.setPositionZ(positionZ);
            bone.setScaleX(scaleX);
            bone.setScaleY(scaleY);
            bone.setScaleZ(scaleZ);
        }
    }
}
