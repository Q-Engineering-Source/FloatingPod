package com.wi.floatingpod.entity;

import net.minecraft.entity.Entity;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.world.World;
import com.wi.floatingpod.vendored.geckolib3.core.IAnimatable;
import com.wi.floatingpod.vendored.geckolib3.core.PlayState;
import com.wi.floatingpod.vendored.geckolib3.core.builder.AnimationBuilder;
import com.wi.floatingpod.vendored.geckolib3.core.controller.AnimationController;
import com.wi.floatingpod.vendored.geckolib3.core.event.predicate.AnimationEvent;
import com.wi.floatingpod.vendored.geckolib3.core.manager.AnimationData;
import com.wi.floatingpod.vendored.geckolib3.core.manager.AnimationFactory;

/**
 * 浮游辅助单元的世界实体本体。
 *
 * <p>本类位于 common 侧，只依赖 vanilla {@link Entity} 与 GeckoLib 的 {@code core} 包。
 * {@code core} 包内的动画类型不引用任何 {@code net.minecraft.client} 符号，因此专用服务端加载本类
 * 不会牵出 Gecko 客户端类，这是 {@code doc/01-technical-baseline.md} 门禁 3
 * 「服务端路径不得加载 Gecko 客户端类」的结构前提。
 *
 * <p>本类刻意不持有渲染器、模型或贴图引用。模型与渲染只在客户端 proxy 装配，
 * 与 {@code doc/02-architecture.md}「该层不依赖客户端渲染类」一致。
 */
public class FloatingPodEntity extends Entity implements IAnimatable {

    /** 动画控制器名称。GeckoLib 以该名称作为控制器在 {@link AnimationData} 中的键。 */
    private static final String ANIMATION_CONTROLLER_NAME = "floatingpod_main_controller";

    /**
     * 待机动画名称，必须与 {@code animations/general_purpose_support_drone_001.animation.json}
     * 中的动画键完全一致；不一致时 GeckoLib 会在客户端报告找不到该动画。
     */
    private static final String IDLE_ANIMATION_NAME = "idle";

    /** 动画间过渡时长，单位为 tick。当前只有一条待机动画，过渡时长不影响表现。 */
    private static final float ANIMATION_TRANSITION_TICKS = 5.0F;

    /**
     * 实体碰撞箱边长。当前取 1.0，仅用于让实体在世界中具备可选中的体积；
     * {@code doc/} 尚未规定辅助机的碰撞尺寸，最终值待相应切片确认。
     */
    private static final float BOUNDING_BOX_SIZE = 1.0F;

    /**
     * GeckoLib 的动画数据工厂。每个实体实例持有自己的工厂，
     * 由 GeckoLib 在渲染帧中通过 {@link #getFactory()} 取出对应的 {@link AnimationData}。
     */
    private final AnimationFactory animationFactory = new AnimationFactory(this);

    /**
     * 供 Forge 实体注册与世界反序列化使用的构造器。
     *
     * @param world 实体所属世界；服务端与客户端各自持有对应实例。
     */
    public FloatingPodEntity(World world) {
        super(world);
        setSize(BOUNDING_BOX_SIZE, BOUNDING_BOX_SIZE);
    }

    /**
     * 注册同步数据字段。
     *
     * <p>当前切片只验证门禁 5 的两条渲染路径，实体尚无需要同步到客户端的权威状态字段。
     * 服务端权威的释放、收回与动画预设切换属于 {@code doc/05-lifecycle-and-interactions.md}
     * 的后续切片，届时在此登记对应的 {@code DataParameter}。
     */
    @Override
    protected void entityInit() {
    }

    /**
     * 从 NBT 恢复实体状态。
     *
     * <p>按 {@code doc/02-architecture.md}「数据库是唯一恢复存储」，辅助机的所有权、模块与状态版本
     * 不经实体 NBT 往返，因此本方法当前不读取任何字段。恢复权威由持久化层负责，
     * 不能从实体 NBT 猜测所有权或版本。
     *
     * @param compound 世界存档提供的实体标签。
     */
    @Override
    protected void readEntityFromNBT(NBTTagCompound compound) {
    }

    /**
     * 将实体状态写入 NBT。
     *
     * <p>与 {@link #readEntityFromNBT(NBTTagCompound)} 同因：领域状态由数据库承担恢复权威，
     * 实体 NBT 不作为第二个权威来源，故当前不写入任何字段。
     *
     * @param compound 世界存档提供的实体标签。
     */
    @Override
    protected void writeEntityToNBT(NBTTagCompound compound) {
    }

    /**
     * 向 GeckoLib 登记动画控制器。GeckoLib 在首次为本实体创建 {@link AnimationData} 时回调本方法；
     * 未登记控制器时其动画判定永远不会被调用。
     *
     * @param data GeckoLib 为本实体维护的动画数据容器。
     */
    @Override
    public void registerControllers(AnimationData data) {
        data.addAnimationController(new AnimationController<>(
                this,
                ANIMATION_CONTROLLER_NAME,
                ANIMATION_TRANSITION_TICKS,
                this::selectAnimation));
    }

    /**
     * 提供 GeckoLib 所需的动画数据工厂。
     *
     * @return 本实体实例专属的动画工厂。
     */
    @Override
    public AnimationFactory getFactory() {
        return animationFactory;
    }

    /**
     * 选择当前应播放的动画。
     *
     * <p>当前只循环播放待机动画。按 {@code doc/05-lifecycle-and-interactions.md}，
     * 释放与收回的动画预设由服务端决定、客户端不得自行构造状态切换；
     * 该状态机属后续切片，本切片不引入任何客户端自主的动画选择逻辑。
     *
     * @param event GeckoLib 每渲染帧提供的动画事件。
     * @return 恒为 {@link PlayState#CONTINUE}，表示该控制器持续播放。
     */
    private PlayState selectAnimation(AnimationEvent<FloatingPodEntity> event) {
        event.getController().setAnimation(new AnimationBuilder().loop(IDLE_ANIMATION_NAME));
        return PlayState.CONTINUE;
    }
}
