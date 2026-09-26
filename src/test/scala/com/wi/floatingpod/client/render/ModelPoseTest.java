package com.wi.floatingpod.client.render;

import com.wi.floatingpod.vendored.geckolib3.geo.render.built.GeoBone;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 直接验证辉光绘制使用的不可变模型姿势。
 */
class ModelPoseTest {

    /** 浮点变换断言的精确容差。 */
    private static final float EPSILON = 0.0F;

    /**
     * 验证两个实体的独立姿势可在共享骨骼上反复回放，且覆盖全部九个变换分量。
     */
    @Test
    void capturesAndReappliesIndependentSharedBonePoses() {
        GeoBone body = bone("body");
        GeoBone bloom = bone("bloom_core");

        setTransform(body, 1.0F);
        setTransform(bloom, 11.0F);
        ModelPose first = ModelPose.capture(Arrays.asList(body, bloom));

        setTransform(body, 21.0F);
        setTransform(bloom, 31.0F);
        ModelPose second = ModelPose.capture(Arrays.asList(body, bloom));

        first.apply(Arrays.asList(body, bloom));
        assertTransform(body, 1.0F);
        assertTransform(bloom, 11.0F);

        second.apply(Arrays.asList(body, bloom));
        assertTransform(body, 21.0F);
        assertTransform(bloom, 31.0F);

        first.apply(Arrays.asList(body, bloom));
        assertTransform(body, 1.0F);
        assertTransform(bloom, 11.0F);
    }

    /**
     * 验证模型布局变化时立即失败，避免把旧姿势静默写入错误骨骼。
     */
    @Test
    void rejectsMismatchedBoneLayout() {
        GeoBone original = bone("body");
        ModelPose pose = ModelPose.capture(Collections.singletonList(original));

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> pose.apply(Collections.singletonList(bone("other"))));
        assertEquals("模型姿势骨骼不匹配: index=0, expected=body, actual=other", exception.getMessage());
    }

    /**
     * 创建具名测试骨骼。
     *
     * @param name 骨骼名称
     * @return 具名骨骼
     */
    private static GeoBone bone(String name) {
        GeoBone bone = new GeoBone();
        bone.setModelRendererName(name);
        return bone;
    }

    /**
     * 从给定基值写入九个彼此不同的变换分量。
     *
     * @param bone 目标骨骼
     * @param base 第一个分量的基值
     */
    private static void setTransform(GeoBone bone, float base) {
        bone.setRotationX(base);
        bone.setRotationY(base + 1.0F);
        bone.setRotationZ(base + 2.0F);
        bone.setPositionX(base + 3.0F);
        bone.setPositionY(base + 4.0F);
        bone.setPositionZ(base + 5.0F);
        bone.setScaleX(base + 6.0F);
        bone.setScaleY(base + 7.0F);
        bone.setScaleZ(base + 8.0F);
    }

    /**
     * 断言骨骼的九个变换分量与给定基值一致。
     *
     * @param bone 待断言骨骼
     * @param base 第一个分量的基值
     */
    private static void assertTransform(GeoBone bone, float base) {
        assertEquals(base, bone.getRotationX(), EPSILON);
        assertEquals(base + 1.0F, bone.getRotationY(), EPSILON);
        assertEquals(base + 2.0F, bone.getRotationZ(), EPSILON);
        assertEquals(base + 3.0F, bone.getPositionX(), EPSILON);
        assertEquals(base + 4.0F, bone.getPositionY(), EPSILON);
        assertEquals(base + 5.0F, bone.getPositionZ(), EPSILON);
        assertEquals(base + 6.0F, bone.getScaleX(), EPSILON);
        assertEquals(base + 7.0F, bone.getScaleY(), EPSILON);
        assertEquals(base + 8.0F, bone.getScaleZ(), EPSILON);
    }
}
