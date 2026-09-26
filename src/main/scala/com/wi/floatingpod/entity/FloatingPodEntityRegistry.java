package com.wi.floatingpod.entity;

import com.wi.floatingpod.FloatingPod;
import net.minecraft.util.ResourceLocation;
import net.minecraftforge.event.RegistryEvent;
import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.fml.common.registry.EntityEntry;
import net.minecraftforge.fml.common.registry.EntityEntryBuilder;
import net.minecraftforge.fml.common.eventhandler.SubscribeEvent;

/**
 * 浮游辅助单元实体的注册入口。
 *
 * <p>注册发生在 common 侧：服务端与客户端必须使用同一个实体名与网络 ID，
 * 否则生成包无法在两侧对应到同一实体类型。本类不引用任何渲染类型，
 * 渲染器绑定由客户端装配实现单独完成。
 *
 * <p>使用 {@link EntityEntryBuilder} 而非 {@code EntityRegistry.registerModEntity}：
 * 后者在 Cleanroom 0.6.8-alpha 中两个重载均已标记 {@code @Deprecated}，
 * 现行入口是注册表事件配合该 Builder，这也与项目「大对象构造优先采用 Builder 模式」一致。
 */
@Mod.EventBusSubscriber(modid = FloatingPod.MOD_ID)
public final class FloatingPodEntityRegistry {

    /** 实体的注册名，与 mod id 一起构成全局唯一的 {@link ResourceLocation}。 */
    public static final String ENTITY_NAME = "floating_pod";

    /**
     * 实体在本 mod 内的网络 ID。该 ID 只需在 FloatingPod 自身注册的实体之间唯一，
     * 由 Forge 与 mod id 组合成全局标识。
     */
    private static final int NETWORK_ID = 1;

    /**
     * 客户端开始接收实体更新包的追踪距离，单位为方块。
     * 取 64 与 vanilla 常规生物一致，确保辅助机在玩家可见范围内持续同步。
     */
    private static final int TRACKING_RANGE = 64;

    /** 实体更新包的发送间隔，单位为 tick。取 3 与 vanilla 常规生物一致。 */
    private static final int UPDATE_FREQUENCY = 3;

    /** 是否发送速度更新。辅助机后续需要平滑跟随移动，因此保留速度同步。 */
    private static final boolean SENDS_VELOCITY_UPDATES = true;

    /**
     * 本类只提供静态注册回调，不允许实例化。
     */
    private FloatingPodEntityRegistry() {
    }

    /**
     * 在 Forge 的实体注册表事件中登记浮游辅助单元。
     *
     * <p>由 {@link Mod.EventBusSubscriber} 自动订阅，两端都会触发，保证网络 ID 一致。
     *
     * @param event Forge 提供的实体注册表事件。
     */
    @SubscribeEvent
    public static void onRegisterEntities(RegistryEvent.Register<EntityEntry> event) {
        event.getRegistry().register(EntityEntryBuilder.<FloatingPodEntity>create()
                .entity(FloatingPodEntity.class)
                .id(new ResourceLocation(FloatingPod.MOD_ID, ENTITY_NAME), NETWORK_ID)
                .name(ENTITY_NAME)
                .tracker(TRACKING_RANGE, UPDATE_FREQUENCY, SENDS_VELOCITY_UPDATES)
                .factory(FloatingPodEntity::new)
                .build());
    }
}
