package com.wi.floatingpod.client.render;

import gregtech.client.renderer.IRenderSetup;
import gregtech.client.shader.postprocessing.BloomType;
import gregtech.client.utils.BloomEffectUtil;
import gregtech.client.utils.EffectRenderContext;
import gregtech.client.utils.IBloomEffect;
import net.minecraft.client.Minecraft;
import net.minecraft.client.renderer.BufferBuilder;
import net.minecraft.client.renderer.EntityRenderer;
import net.minecraft.client.renderer.RenderGlobal;
import net.minecraft.entity.Entity;
import net.minecraft.util.BlockRenderLayer;

import java.util.Objects;

/**
 * Lumenized 辉光注册与回调的桥接实现。
 *
 * <p><b>类加载隔离</b>：本类是本项目**唯一**引用 {@code gregtech} 包类型的类。Lumenized 是
 * {@code doc/01-technical-baseline.md} 规定的客户端侧**可选**前置，构建时以 {@code compileOnly}
 * 解析签名、不进产物，执行期不保证存在。因此凡常量池含 {@code gregtech/} 符号的类在 Lumenized
 * 缺失时一旦被加载就会抛 {@code NoClassDefFoundError}。把这些引用全部收敛到本类，
 * 并且只在 {@code Loader.isModLoaded("lumenized")} 为真时才触达本类，即可使缺失场景下
 * 本类根本不被解析。渲染器、模型、客户端装配入口与实体类均不引用本类型，只经
 * {@link BloomPainterBinding} 与 {@link BloomQueue} 这两个中立类型交互。
 *
 * <p>Lumenized 缺失时不注册辉光、只保留伪发光，这不是静默降级，而是
 * {@code doc/01-technical-baseline.md} 明确规定的行为：以 {@code bloom} 开头的 bone 在任何情况下
 * 都呈现伪发光，Lumenized 可用时在此基础上**叠加**辉光，缺失时保留伪发光但不叠加，
 * 且均不影响模型、动画或实体功能。
 *
 * <p>本类同时实现 {@link IRenderSetup} 与 {@link IBloomEffect}。实测 Lumenized 的私有
 * {@code draw} 对每个 ticket 依次执行 {@code checkValidity} → {@code isValid} 过滤 →
 * {@code shouldRenderBloomEffect} 过滤 → {@code renderBloomEffect}，其中 {@code preDraw}
 * 由一个布尔标志位守卫、每个 ticket 列表只在首个实际通过过滤的 ticket 之前调用一次，
 * {@code postDraw} 在列表结束后调用一次。
 *
 * <p><b>每帧消费两次</b>：默认配置下（实测 {@code LumenizedConfig} 的 {@code static{}} 中
 * {@code emissiveTexturesBloom} 为 {@code true}、{@code bloomStyle} 为 {@code 2}）
 * {@code renderBloomBlockLayer} 走 bloom FBO 路径，其内对本 ticket 调用两次私有 {@code draw}：
 * 第一次（offset 400）在绑定主帧缓冲后执行，使自发光几何在世界中可见；
 * 第二次（offset 790）在绑定 {@code bloomFBO} 后执行，作为辉光后处理的输入。
 * 因此 {@code preDraw} 与 {@code postDraw} 每帧各触发两次，均**不是**队列清空点；
 * 必须等 {@code renderBloomBlockLayer} 整次调用返回后才能清空，见 {@link #render}。
 *
 * <p><b>中立边界</b>：本桥接由 {@link BloomWorldPass} 在半透明方块层与实体第二阶段之间调用。
 * 若 Lumenized 自身的同阶段回调已消费请求，回调与本桥接以同一 pass 标记协作，避免重复绘制。
 */
public final class LumenizedBloomBridgeImpl implements IRenderSetup, IBloomEffect, BloomWorldPass.Fallback {

    /** 非立体渲染时 vanilla 传给 {@code renderWorldPass} 的唯一 pass。 */
    private static final int NORMAL_RENDER_PASS = 2;

    /** 没有记录到标准回调消费时使用的哨兵值。 */
    private static final int NO_RENDER_PASS = -1;

    /** 本帧待绘制的辉光请求来源，由实体渲染器在渲染阶段填充。 */
    private final BloomQueue queue;

    /** 实际执行绘制的渲染器的延迟绑定入口。 */
    private final BloomPainterBinding binding;

    /** 当前世界 pass 是否已经由 Lumenized 注入回调成功消费。 */
    private int upstreamConsumedRenderPass = NO_RENDER_PASS;

    /**
     * 创建桥接。
     *
     * @param queue   与实体渲染器共享的同一个队列实例。
     * @param binding 与实体渲染器共享的同一个绑定实例。
     */
    public LumenizedBloomBridgeImpl(BloomQueue queue, BloomPainterBinding binding) {
        this.queue = Objects.requireNonNull(queue, "queue");
        this.binding = Objects.requireNonNull(binding, "binding");
    }

    /**
     * 向 Lumenized 注册本桥接，使其参与辉光绘制阶段。
     *
     * <p>选用 {@code BloomType.UNREAL}：它是 Lumenized 从 GTCEu 继承的辉光算法之一，
     * 也是既有 1.12.2 实现在同类模型辉光上采用的类型。
     *
     * <p>validity checker 恒为有效：本桥接的生命周期与客户端会话一致，不随实体增减失效。
     * 实体是否需要绘制由每帧队列决定，而非由 ticket 有效性决定，二者是不同层面的判定。
     *
     * <p>实测 {@code registerBloomRender} 在检测到 Optifine shader pack 已加载时直接返回
     * {@code null} 而不注册。这是 Lumenized 自身的行为：其辉光后处理与 shader pack 不共存。
     * 该情形下伪发光基线仍然生效，符合上述 {@code doc/} 规定，因此不视为错误；
     * 但也不静默忽略，返回值原样交由调用方判定与记录。
     *
     * @return Lumenized 返回的 ticket；当其因 shader pack 已加载而拒绝注册时为 {@code null}。
     */
    public BloomEffectUtil.BloomRenderTicket register() {
        return BloomEffectUtil.registerBloomRender(this, BloomType.UNREAL, this, candidate -> true);
    }

    /**
     * 辉光批次开始前的状态准备。
     *
     * <p>本实现不改动任何 GL 状态：辉光几何的定位、光照与批次边界全部由
     * {@link #renderBloomEffect} 内部自行管理；Lumenized 的
     * {@code renderBloomBlockLayer} 拥有其 FBO 与 GL 生命周期。
     *
     * @param bufferBuilder Lumenized 提供的共享顶点收集器。
     */
    @Override
    public void preDraw(BufferBuilder bufferBuilder) {
    }

    /**
     * 辉光批次结束后的状态还原。
     *
     * <p>与 {@link #preDraw} 对应，本实现无状态需要还原。
     *
     * @param bufferBuilder Lumenized 提供的共享顶点收集器。
     */
    @Override
    public void postDraw(BufferBuilder bufferBuilder) {
    }

    /**
     * 是否需要在本帧参与辉光绘制。
     *
     * <p>以本帧队列是否为空作为判据：没有实体进入视野时跳过，避免为空内容执行绘制状态切换。
     *
     * @param context 辉光渲染上下文。
     * @return 本帧有待绘制请求时返回 {@code true}。
     */
    @Override
    public boolean shouldRenderBloomEffect(EffectRenderContext context) {
        return !queue.isEmpty();
    }

    /**
     * 绘制本帧全部待辉光的实体。
     *
     * <p>此处只做坐标系换算的参数传递：把 Lumenized 上下文给出的摄像机世界坐标交给绘制者，
     * 由其把排队时记录的实体世界坐标换算为相对摄像机的偏移。实测
     * {@code EffectRenderContext.update} 计算摄像机坐标的公式为
     * {@code lastTickPos + (pos - lastTickPos) * partialTicks}，与 vanilla
     * {@code RenderGlobal.renderEntities} 设定 {@code RenderManager.renderPosX/Y/Z} 的公式逐字相同，
     * 因此该换算与实体渲染阶段所用原点是同一个，不引入偏移。
     *
     * <p>不在此处调用 {@code Tessellator.begin}：Lumenized 传入的是
     * {@code Tessellator.getInstance().getBuffer()} 这一共享实例且未对其调用 {@code begin}，
     * 批次边界由绘制者自行开闭，与伪发光两趟分批的处理方式一致。
     *
     * @param bufferBuilder Lumenized 提供的共享顶点收集器。
     * @param context       辉光渲染上下文，提供摄像机位置与插值比例。
     */
    @Override
    public void renderBloomEffect(BufferBuilder bufferBuilder, EffectRenderContext context) {
        if (queue.isEmpty()) {
            return;
        }
        queue.paintInto(binding.painter(), context.cameraX(), context.cameraY(), context.cameraZ());
        upstreamConsumedRenderPass = currentRenderPass();
    }

    /**
     * 在中立世界 pass 边界补偿未触发的 Lumenized 回调。
     *
     * <p>每次 world pass 结束此方法时都清理请求并复位标记，避免上一个 pass 的数据流入后续 pass。
     *
     * @param renderGlobal 世界区块渲染器。
     * @param renderViewEntity 当前视角实体。
     * @param partialTicks 当前帧的插值比例。
     * @param renderPass vanilla 世界渲染 pass。
     */
    @Override
    public void render(
            RenderGlobal renderGlobal,
            Entity renderViewEntity,
            float partialTicks,
            int renderPass) {
        try {
            if (!queue.isEmpty() && upstreamConsumedRenderPass != renderPass) {
                BloomEffectUtil.renderBloomBlockLayer(
                        renderGlobal,
                        BlockRenderLayer.TRANSLUCENT,
                        partialTicks,
                        renderPass,
                        renderViewEntity);
            }
        } finally {
            queue.clear();
            upstreamConsumedRenderPass = NO_RENDER_PASS;
        }
    }

    /**
     * 取得与 Lumenized 回调相同的 vanilla 世界 pass。
     *
     * @return 当前 {@code renderWorldPass} 的 pass。
     */
    private static int currentRenderPass() {
        Minecraft minecraft = Minecraft.getMinecraft();
        if (minecraft.gameSettings.anaglyph) {
            return EntityRenderer.anaglyphField;
        }
        return NORMAL_RENDER_PASS;
    }
}
