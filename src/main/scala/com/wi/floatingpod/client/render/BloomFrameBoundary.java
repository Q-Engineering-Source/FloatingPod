package com.wi.floatingpod.client.render;

import net.minecraftforge.fml.common.gameevent.TickEvent;
import net.minecraftforge.fml.common.eventhandler.SubscribeEvent;

import java.util.Objects;

/**
 * 辉光请求队列的帧边界。
 *
 * <p>存在动机：辉光队列的清空时机必须与**帧**对齐，而不能与消费或绘制批次对齐。
 * Lumenized 在默认配置下每帧消费同一个 ticket **两次**——第一次绑定主帧缓冲使自发光几何
 * 在世界中可见，第二次绑定 {@code bloomFBO} 作为辉光后处理的输入。若在任何一次消费中清空，
 * 第二次就会因队列为空而被 {@code shouldRenderBloomEffect} 整体跳过，辉光不再出现。
 *
 * <p>因此清空放在 {@code TickEvent.RenderTickEvent} 的 {@code Phase.START}。实测 vanilla
 * {@code Minecraft.runGameLoop} 的调用序为 {@code onRenderTickStart}（offset 274）→
 * {@code EntityRenderer.updateCameraAndRender}（offset 313）→ {@code onRenderTickEnd}（offset 358），
 * 而 Lumenized 的辉光注入点位于世界渲染内部。故 {@code Phase.START} 严格早于本帧的两次消费，
 * 清空的是上一帧的残留，本帧入队的几何不受影响。
 *
 * <p>不用 {@code Phase.END} 清空：世界渲染期间若抛出异常，{@code onRenderTickEnd} 不会执行，
 * 残留会跨帧累积。{@code Phase.START} 在每帧开头无条件执行，即使上一帧异常中断也能收敛。
 *
 * <p>也不用 {@code EffectRenderContext.partialTicks()} 的变化判定新帧：帧率与 tick 率的关系
 * 不保证该值在相邻帧必然不同，相等时会被误判为同一帧而漏清。
 *
 * <p>本类刻意不引用任何 Lumenized 类型，只持有中立的 {@link BloomQueue}，
 * 因此不影响客户端类加载隔离。它只在 Lumenized 已加载且辉光注册成功时才被注册到事件总线。
 */
public final class BloomFrameBoundary {

    /** 被维护的队列，与实体渲染器和辉光桥接共享同一实例。 */
    private final BloomQueue queue;

    /**
     * 创建帧边界处理器。
     *
     * @param queue 与实体渲染器共享的同一个队列实例。
     */
    public BloomFrameBoundary(BloomQueue queue) {
        this.queue = Objects.requireNonNull(queue, "queue");
    }

    /**
     * 在每帧渲染开始前清空上一帧的辉光请求。
     *
     * <p>只处理 {@code START} 阶段。同一个 {@code RenderTickEvent} 类型会在一帧内以
     * {@code START} 与 {@code END} 各投递一次，不作阶段判定会导致本帧入队的几何在
     * {@code END} 阶段被清掉，虽不影响本帧显示，却使队列状态与帧的对应关系变得不可推理。
     *
     * @param event Forge 投递的渲染帧事件。
     */
    @SubscribeEvent
    public void onRenderTick(TickEvent.RenderTickEvent event) {
        if (event.phase == TickEvent.Phase.START) {
            queue.clear();
        }
    }
}
