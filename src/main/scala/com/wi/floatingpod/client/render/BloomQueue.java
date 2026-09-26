package com.wi.floatingpod.client.render;

import java.util.ArrayList;
import java.util.List;

/**
 * 本帧待绘制辉光的请求队列。
 *
 * <p>存在动机：Lumenized 的辉光回调不在实体渲染阶段。实测其 mixin 注入于
 * {@code EntityRenderer.renderWorldPass} 中 {@code renderBlockLayer} 第四次调用
 * （{@code TRANSLUCENT}，ordinal 为 3）之后，位于 {@code renderEntities} 的 pass 0 之后、pass 1
 * 之前。中立世界 pass hook 在同一边界消费已登记的请求。
 *
 * <p>本类刻意不引用任何 Lumenized 类型，使排队路径在 Lumenized 缺失时也能安全加载。
 * Lumenized 缺失时不注册桥接，因而无人消费本队列；此时渲染器也不会入队，
 * 见 {@code ClientSetupImpl} 的注册守卫与渲染器的入队条件。
 *
 * <p>只在客户端渲染线程访问，故不作同步。该约束由使用位置保证：
 * {@link #enqueue} 只在实体渲染中调用，{@link #paintInto} 只在辉光回调中调用，
 * {@link #clear} 在中立世界 pass hook 边界及渲染帧开始的恢复边界调用，三者同为渲染线程。
 */
public final class BloomQueue {

    /**
     * 本帧已登记的请求。
     *
     * <p>用 {@link List} 而非 {@code Set}：同一实体在一帧内只被 pass 0 渲染一次，
     * 且列表的迭代顺序稳定，便于取证时对照。
     */
    private final List<BloomRequest> pending = new ArrayList<>();

    /**
     * 创建空队列。
     */
    public BloomQueue() {
    }

    /**
     * 登记一个本帧的辉光绘制请求。
     *
     * @param request 实体渲染阶段捕获的定位与动画参数。
     */
    public void enqueue(BloomRequest request) {
        pending.add(request);
    }

    /**
     * 是否本帧无待绘制请求。
     *
     * <p>供辉光桥接在回调前判定，使空队列时不必执行任何绘制状态切换。
     *
     * @return 队列为空时返回 {@code true}。
     */
    public boolean isEmpty() {
        return pending.isEmpty();
    }

    /**
     * 把本帧队列逐个交由 {@link BloomPainter} 绘制，**不清空**队列。
     *
     * <p>不清空是本方法的关键契约，理由是 Lumenized 每帧会消费本 ticket **两次**，
     * 而两次的绘制目标不同，缺一不可：
     *
     * <ol>
     *   <li>第一次绑定主帧缓冲 {@code mc.getFramebuffer()}，使自发光几何在世界中可见；</li>
     *   <li>第二次绑定 {@code bloomFBO}，作为辉光后处理的输入。</li>
     * </ol>
     *
     * <p>若在本方法内清空，第二次消费时 {@code shouldRenderBloomEffect} 会因队列为空而返回
     * {@code false}，ticket 被整体跳过，{@code bloomFBO} 拿不到任何几何，辉光后处理无输入。
     * 其净结果是伪发光正常但辉光完全不出现，且与 Lumenized 缺失时的正确降级在视觉上无法区分。
     *
     * <p>中立 world-pass hook 边界的一次完整 Lumenized 调用结束后由 {@link #clear} 清空；
     * {@link BloomFrameBoundary} 的帧开始清空只负责恢复异常中断或未进入世界渲染时的残留。
     * 这里不捕获异常，绘制失败照常向上抛出，符合 Fail Fast。
     *
     * @param painter 实际执行绘制的实现，即实体渲染器。
     * @param cameraX 摄像机插值后的世界 X 坐标。
     * @param cameraY 摄像机插值后的世界 Y 坐标。
     * @param cameraZ 摄像机插值后的世界 Z 坐标。
     */
    public void paintInto(BloomPainter painter, double cameraX, double cameraY, double cameraZ) {
        for (BloomRequest request : pending) {
            painter.paintBloom(request, cameraX, cameraY, cameraZ);
        }
    }

    /**
     * 清空队列，由中立世界 pass hook 边界或帧开始恢复边界调用。
     *
     * <p>清空时机必须晚于一次完整的 {@code renderBloomBlockLayer} 调用，而不能发生在单次消费或
     * 单个绘制批次之间：该调用内存在两次同步消费，二者都必须看到完整队列。见 {@link #paintInto}。
     *
     * <p>即使当前世界 pass 因异常未抵达清理边界，或该帧未进入世界渲染，本方法仍会在下一帧的
     * {@link BloomFrameBoundary} 中执行，因此队列不会跨帧泄漏或无界增长。
     */
    public void clear() {
        pending.clear();
    }
}
