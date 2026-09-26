package com.wi.floatingpod.client.render;

import com.wi.floatingpod.entity.FloatingPodEntity;
import com.wi.floatingpod.vendored.geckolib3.core.event.predicate.AnimationEvent;
import com.wi.floatingpod.vendored.geckolib3.core.util.Color;
import com.wi.floatingpod.vendored.geckolib3.geo.render.built.GeoBone;
import com.wi.floatingpod.vendored.geckolib3.geo.render.built.GeoCube;
import com.wi.floatingpod.vendored.geckolib3.geo.render.built.GeoModel;
import com.wi.floatingpod.vendored.geckolib3.geo.render.built.GeoQuad;
import com.wi.floatingpod.vendored.geckolib3.geo.render.built.GeoVertex;
import com.wi.floatingpod.vendored.geckolib3.model.provider.GeoModelProvider;
import com.wi.floatingpod.vendored.geckolib3.renderers.geo.IGeoRenderer;
import net.minecraft.client.renderer.BufferBuilder;
import net.minecraft.client.renderer.GlStateManager;
import net.minecraft.client.renderer.Tessellator;
import net.minecraft.client.renderer.entity.Render;
import net.minecraft.client.renderer.entity.RenderManager;
import net.minecraft.client.renderer.vertex.DefaultVertexFormats;
import net.minecraft.util.ResourceLocation;
import org.lwjgl.opengl.GL11;

import javax.vecmath.Vector3f;
import javax.vecmath.Vector4f;
import java.util.Collections;

/**
 * 浮游辅助单元的实体渲染器。
 *
 * <p>不继承 GeckoLib 的 {@code GeoEntityRenderer}：后者的类型上界是
 * {@code EntityLivingBase & IAnimatable}，其 {@code doRender} 依赖生命值、伤害闪红、死亡旋转、
 * 拴绳与幼体缩放等生物语义，而浮游辅助单元不是生物。因此直接在 {@link Render} 上实现
 * {@link IGeoRenderer}，只取所需的模型绘制能力。
 *
 * <p>由此产生一处必须补偿的后果：{@code GeoEntityRenderer} 的静态初始化块负责注册
 * {@code AnimationController.addModelFetcher}，不继承它时该注册不会发生，动画不会运行。
 * 该注册改由客户端装配入口显式完成。
 *
 * <p>核心职责是把模型分成两个绘制批次以实现伪发光。光照贴图坐标是绘制时刻的 OpenGL 状态，
 * 不是逐顶点数据，因此必须让 bloom 几何独占一个 {@code begin}/{@code draw} 批次，
 * 才能只对它施加满亮度。
 *
 * <p>本类同时实现 {@link BloomPainter}，在 Lumenized 可用时于其辉光阶段再绘制一遍 bloom 子树，
 * 使辉光叠加在伪发光之上。该实现刻意不引用任何 Lumenized 类型，
 * 因此 Lumenized 缺失时本类仍可正常加载，伪发光路径不受影响。
 */
public class FloatingPodRenderer extends Render<FloatingPodEntity>
        implements IGeoRenderer<FloatingPodEntity>, BloomPainter {

    /** 判定伪发光 bone 的名称前缀，由 {@code doc/01-technical-baseline.md} 规定。 */
    private static final String BLOOM_BONE_PREFIX = "bloom";

    /** 模型整体的垂直微调量，避免模型底面与地面共面导致的深度冲突。 */
    private static final float VERTICAL_OFFSET = 0.01F;

    /** 颜色分量的满值，用于把 GeckoLib 的 0-255 色值换算为 0-1 的浮点值。 */
    private static final float COLOR_COMPONENT_MAX = 255.0F;

    /** 提供模型、贴图与动画资源位置，并驱动动画处理。 */
    private final FloatingPodModel modelProvider;

    /**
     * 本帧辉光请求队列。
     *
     * <p>为 {@code null} 表示 Lumenized 未加载，本渲染器不参与辉光排队，只走伪发光路径。
     * 这不是静默降级，而是 {@code doc/01-technical-baseline.md} 规定的行为。
     */
    private final BloomQueue bloomQueue;

    /**
     * 创建渲染器。
     *
     * @param renderManager vanilla 渲染管理器，由实体渲染工厂提供。
     * @param modelProvider 本实体的 GeckoLib 模型提供器。
     * @param bloomQueue    辉光请求队列；Lumenized 未加载时为 {@code null}。
     * @param bloomBinding  辉光绘制者绑定入口；Lumenized 未加载时为 {@code null}。
     */
    public FloatingPodRenderer(RenderManager renderManager, FloatingPodModel modelProvider,
                               BloomQueue bloomQueue, BloomPainterBinding bloomBinding) {
        super(renderManager);
        this.modelProvider = modelProvider;
        this.bloomQueue = bloomQueue;
        if (bloomBinding != null) {
            bloomBinding.bind(this);
        }
    }

    /**
     * 绘制实体。负责坐标变换、动画推进与贴图绑定，随后交由 {@link #render} 完成几何绘制。
     *
     * @param entity       待渲染实体。
     * @param x            实体相对摄像机的 X 偏移。
     * @param y            实体相对摄像机的 Y 偏移。
     * @param z            实体相对摄像机的 Z 偏移。
     * @param entityYaw    实体插值后的偏航角。
     * @param partialTicks 当前帧在两个 tick 之间的插值比例。
     */
    @Override
    public void doRender(FloatingPodEntity entity, double x, double y, double z, float entityYaw, float partialTicks) {
        GlStateManager.pushMatrix();
        GlStateManager.translate(x, y + VERTICAL_OFFSET, z);
        GlStateManager.rotate(180.0F - entityYaw, 0.0F, 1.0F, 0.0F);

        GeoModel model = modelProvider.getModel(modelProvider.getModelLocation(entity));
        if (model == null) {
            throw new IllegalStateException(
                    "FloatingPod 模型未加载: " + modelProvider.getModelLocation(entity));
        }
        AnimationEvent<FloatingPodEntity> animationEvent = new AnimationEvent<>(
                entity, 0.0F, 0.0F, partialTicks, false, Collections.emptyList());
        modelProvider.setLivingAnimations(entity, getUniqueID(entity), animationEvent);
        ModelPose bloomPose = bloomQueue == null ? null
                : ModelPose.capture(modelProvider.getAnimationProcessor().getModelRendererList());

        bindEntityTexture(entity);
        Color renderColor = getRenderColor(entity, partialTicks);
        render(model, entity, partialTicks,
                renderColor.getRed() / COLOR_COMPONENT_MAX,
                renderColor.getGreen() / COLOR_COMPONENT_MAX,
                renderColor.getBlue() / COLOR_COMPONENT_MAX,
                renderColor.getAlpha() / COLOR_COMPONENT_MAX);

        GlStateManager.popMatrix();

        enqueueBloom(entity, bloomPose, entityYaw, partialTicks);

        super.doRender(entity, x, y, z, entityYaw, partialTicks);
    }

    /**
     * 登记本帧的辉光绘制请求。
     *
     * <p>存在动机：Lumenized 的辉光回调发生在实体渲染之后（实测注入于
     * {@code renderBlockLayer} 的 {@code TRANSLUCENT} 之后，而实体 pass 0 在其之前），
     * 因此不能在本方法内直接绘制辉光，只能记录定位量，待回调时绘制。
     *
     * <p>记录世界绝对坐标而非 vanilla 传入的相对偏移：相对偏移的原点是
     * {@code RenderManager.renderPosX/Y/Z}，该字段为私有且无访问器，无法读取；
     * 而回调阶段的原点是当时由辉光上下文给出的摄像机坐标。只有绝对坐标在两个阶段都成立。
     *
     * <p>世界坐标由实体自身的插值公式算出，与 vanilla 计算 {@code x/y/z} 时所用公式相同：
     * 实测 {@code RenderManager.renderEntityStatic} 对实体取
     * {@code lastTickPos + (pos - lastTickPos) * partialTicks}，
     * 而 {@code RenderGlobal.renderEntities} 用同一公式在渲染视角实体上求得摄像机位置。
     * {@code EffectRenderContext.update} 亦逐字采用该公式，因此两阶段的原点一致，换算不引入偏移。
     *
     * @param entity       待绘制实体。
     * @param pose         正常动画求值后立即捕获的骨骼姿势。
     * @param entityYaw    实体插值后的偏航角。
     * @param partialTicks 当前帧插值比例。
     */
    private void enqueueBloom(FloatingPodEntity entity, ModelPose pose,
                              float entityYaw, float partialTicks) {
        if (bloomQueue == null) {
            return;
        }
        bloomQueue.enqueue(new BloomRequest(entity, pose,
                interpolate(entity.lastTickPosX, entity.posX, partialTicks),
                interpolate(entity.lastTickPosY, entity.posY, partialTicks),
                interpolate(entity.lastTickPosZ, entity.posZ, partialTicks),
                entityYaw, partialTicks));
    }

    /**
     * 按帧内插值比例在上一 tick 与当前 tick 的值之间线性插值。
     *
     * <p>与 vanilla 渲染路径所用公式一致，使排队记录的位置与本帧实际绘制位置相同。
     *
     * @param previous     上一 tick 的值。
     * @param current      当前 tick 的值。
     * @param partialTicks 帧内插值比例，取值 0 到 1。
     * @return 插值结果。
     */
    private static double interpolate(double previous, double current, float partialTicks) {
        return previous + (current - previous) * partialTicks;
    }

    /**
     * 在 Lumenized 的辉光阶段绘制一个实体的 bloom 子树，使辉光叠加在伪发光之上。
     *
     * <p>此时 OpenGL 矩阵是世界渲染矩阵、原点为摄像机，且不在实体渲染的矩阵栈内，
     * 因此须自行完成入栈、按摄像机偏移定位、旋转，并复现与伪发光批次相同的绘制状态。
     *
     * <p>模型提供器是本实体类型共享的单一实例，其 bone 树只保存最后一次正常渲染的姿态。
     * 本方法必须应用请求随正常实体渲染捕获的不可变姿势，不能再次推进动画；再次推进会重复进入
     * {@code AnimationController.process}，消耗控制器转换队列并使循环动画停止。
     *
     * <p>绘制走与伪发光完全相同的 {@link #drawPass}（{@code bloomPass} 为 {@code true}），
     * 因此缺面判空、子树继承语义与批次边界都与第一条路径一致，不会随资产变化而分叉。
     * 同时仍在 {@link EmissiveLightmapScope} 内绘制：辉光是在伪发光之上叠加，
     * 这一遍本身也必须是满亮度的，否则叠加结果会被世界光照压暗。
     *
     * @param request 实体渲染阶段捕获的定位与动画参数。
     * @param cameraX 摄像机插值后的世界 X 坐标。
     * @param cameraY 摄像机插值后的世界 Y 坐标。
     * @param cameraZ 摄像机插值后的世界 Z 坐标。
     */
    @Override
    public void paintBloom(BloomRequest request, double cameraX, double cameraY, double cameraZ) {
        FloatingPodEntity entity = request.entity();
        GeoModel model = modelProvider.getModel(modelProvider.getModelLocation(entity));
        if (model == null) {
            throw new IllegalStateException(
                    "FloatingPod 模型未加载: " + modelProvider.getModelLocation(entity));
        }

        request.pose().apply(modelProvider.getAnimationProcessor().getModelRendererList());

        bindEntityTexture(entity);
        Color renderColor = getRenderColor(entity, request.partialTicks());

        GlStateManager.pushMatrix();
        GlStateManager.translate(
                request.worldX() - cameraX,
                request.worldY() - cameraY + VERTICAL_OFFSET,
                request.worldZ() - cameraZ);
        GlStateManager.rotate(180.0F - request.entityYaw(), 0.0F, 1.0F, 0.0F);

        GlStateManager.disableCull();
        GlStateManager.enableRescaleNormal();
        try (EmissiveLightmapScope emissiveScope = new EmissiveLightmapScope()) {
            drawPass(model,
                    renderColor.getRed() / COLOR_COMPONENT_MAX,
                    renderColor.getGreen() / COLOR_COMPONENT_MAX,
                    renderColor.getBlue() / COLOR_COMPONENT_MAX,
                    renderColor.getAlpha() / COLOR_COMPONENT_MAX,
                    true);
        } finally {
            GlStateManager.disableRescaleNormal();
            GlStateManager.enableCull();
            GlStateManager.popMatrix();
        }
    }

    /**
     * 分两个批次绘制模型，覆写 {@link IGeoRenderer} 的默认单批次实现。
     *
     * <p>默认实现把全部 bone 收进同一个 {@link BufferBuilder} 并只调用一次
     * {@code Tessellator.draw()}。由于光照贴图坐标是绘制时刻的全局 OpenGL 状态，
     * 单批次下无法只让 bloom bone 满亮度。因此本实现遍历两趟：
     *
     * <ol>
     *   <li>第一趟只收非 bloom 子树，按世界光照绘制；</li>
     *   <li>第二趟只收 bloom 子树，在 {@link EmissiveLightmapScope} 内绘制，得到满亮度伪发光。</li>
     * </ol>
     *
     * <p>第二趟即门禁 5 的伪发光基线，只依赖 vanilla {@code OpenGlHelper}，不依赖任何辉光后处理。
     *
     * @param model        已解析的模型几何。
     * @param animatable   待渲染实体。
     * @param partialTicks 当前帧插值比例。
     * @param red          颜色红色分量，取值 0 到 1。
     * @param green        颜色绿色分量，取值 0 到 1。
     * @param blue         颜色蓝色分量，取值 0 到 1。
     * @param alpha        颜色透明度分量，取值 0 到 1。
     */
    @Override
    public void render(GeoModel model, FloatingPodEntity animatable, float partialTicks,
                       float red, float green, float blue, float alpha) {
        GlStateManager.disableCull();
        GlStateManager.enableRescaleNormal();

        renderEarly(animatable, partialTicks, red, green, blue, alpha);
        renderLate(animatable, partialTicks, red, green, blue, alpha);

        drawPass(model, red, green, blue, alpha, false);

        try (EmissiveLightmapScope emissiveScope = new EmissiveLightmapScope()) {
            drawPass(model, red, green, blue, alpha, true);
        }

        renderAfter(animatable, partialTicks, red, green, blue, alpha);
        GlStateManager.disableRescaleNormal();
        GlStateManager.enableCull();
    }

    /**
     * 收集并绘制一个批次的几何。
     *
     * <p>每次调用独立执行 {@code begin} 与 {@code draw}，使调用方能在批次之间安全切换光照状态。
     *
     * @param model     已解析的模型几何。
     * @param red       颜色红色分量。
     * @param green     颜色绿色分量。
     * @param blue      颜色蓝色分量。
     * @param alpha     颜色透明度分量。
     * @param bloomPass {@code true} 表示只收集伪发光子树，{@code false} 表示只收集其余部分。
     */
    private void drawPass(GeoModel model, float red, float green, float blue, float alpha, boolean bloomPass) {
        BufferBuilder builder = Tessellator.getInstance().getBuffer();
        builder.begin(GL11.GL_QUADS, DefaultVertexFormats.POSITION_TEX_COLOR_NORMAL);
        for (GeoBone topLevelBone : model.topLevelBones) {
            renderFiltered(builder, topLevelBone, red, green, blue, alpha, bloomPass, false);
        }
        Tessellator.getInstance().draw();
    }

    /**
     * 按伪发光归属过滤地递归绘制 bone。
     *
     * <p>必须完整走完整棵树的矩阵变换，即使某个 bone 不属于当前批次也要继续下降，
     * 否则其子 bone 会丢失父级变换而错位。过滤只作用于是否提交该 bone 自身的 cube。
     *
     * <p>伪发光归属按子树继承：一旦进入以 {@code bloom} 开头的 bone，其整棵子树都视为伪发光。
     * 当前资产的六根 bloom bone 均为叶节点，该继承语义在此资产上不产生额外效果，
     * 但可使资产后续新增子 bone 时行为保持一致。
     *
     * @param builder            当前批次的顶点收集器。
     * @param bone               当前处理的 bone。
     * @param red                颜色红色分量。
     * @param green              颜色绿色分量。
     * @param blue               颜色蓝色分量。
     * @param alpha              颜色透明度分量。
     * @param bloomPass          当前批次是否为伪发光批次。
     * @param inheritedBloom     父级是否已判定为伪发光子树。
     */
    private void renderFiltered(BufferBuilder builder, GeoBone bone, float red, float green, float blue, float alpha,
                                boolean bloomPass, boolean inheritedBloom) {
        boolean isBloomSubtree = inheritedBloom || bone.getName().startsWith(BLOOM_BONE_PREFIX);

        MATRIX_STACK.push();
        MATRIX_STACK.translate(bone);
        MATRIX_STACK.moveToPivot(bone);
        MATRIX_STACK.rotate(bone);
        MATRIX_STACK.scale(bone);
        MATRIX_STACK.moveBackFromPivot(bone);

        if (!bone.isHidden() && isBloomSubtree == bloomPass) {
            for (GeoCube cube : bone.childCubes) {
                MATRIX_STACK.push();
                GlStateManager.pushMatrix();
                submitCube(builder, cube, red, green, blue, alpha);
                GlStateManager.popMatrix();
                MATRIX_STACK.pop();
            }
        }

        if (!bone.childBonesAreHiddenToo()) {
            for (GeoBone childBone : bone.childBones) {
                renderFiltered(builder, childBone, red, green, blue, alpha, bloomPass, isBloomSubtree);
            }
        }

        MATRIX_STACK.pop();
    }

    /**
     * 把单个 cube 的顶点提交到当前批次，替代 {@link IGeoRenderer#renderCube}。
     *
     * <p>存在动机：GeckoLib 3.0.31 的 {@code renderCube} 会对缺面 cube 抛
     * {@code NullPointerException}。其 {@code GeoCube.quads} 是定长 6 的数组，
     * {@code createFromPojoCube} 在 per-face UV 模式下对每个面执行
     * {@code face == null ? null : new GeoQuad(...)} 后无条件填满六个槽位；
     * 而 {@code renderCube} 以 {@code for (GeoQuad quad : cube.quads)} 遍历后直接读
     * {@code quad.normal}，不判空。因此任何含缺面 cube 的模型都会在渲染首帧崩溃，
     * 与本项目的两趟分批无关，用 GeckoLib 原版 {@code renderRecursively} 同样会崩。
     *
     * <p>本方法逐面跳过 {@code null} 槽位。这**不是** {@code AGENTS.md} 所禁止的静默降级：
     * {@code null} 是上游资料结构表达「该面在模型中不存在」的合法状态，由 Blockbench
     * 对不可见内面的刻意优化产生，不是错误或异常状态，因此没有可记录的失败、也没有被掩盖的问题。
     * 若改为抛错或补面，反而会拒绝一个合法模型。本项目资产实测 106 个 cube 全部使用 per-face UV，
     * 其中 79 个面数少于 6，共 203 个空槽位散布在 42 根 bone 中的 37 根。
     *
     * <p>除跳过空槽位外，本方法逐项复制 {@code renderCube} 的全部行为：cube 局部的
     * 轴心平移与旋转三步、扁平 cube 的法线翻转修正、法线与顶点各自的矩阵变换、
     * 以及顶点属性的提交顺序。
     *
     * @param builder 当前批次的顶点收集器，其顶点格式由调用方的 {@code begin} 决定。
     * @param cube    待提交的 cube。
     * @param red     颜色红色分量。
     * @param green   颜色绿色分量。
     * @param blue    颜色蓝色分量。
     * @param alpha   颜色透明度分量。
     */
    private void submitCube(BufferBuilder builder, GeoCube cube, float red, float green, float blue, float alpha) {
        MATRIX_STACK.moveToPivot(cube);
        MATRIX_STACK.rotate(cube);
        MATRIX_STACK.moveBackFromPivot(cube);

        for (GeoQuad quad : cube.quads) {
            if (quad == null) {
                continue;
            }
            submitQuad(builder, cube, quad, red, green, blue, alpha);
        }
    }

    /**
     * 提交单个面的四个顶点。
     *
     * <p>从 {@link #submitCube} 拆出，使空槽位判定与实际顶点提交各自单一职责。
     *
     * @param builder 当前批次的顶点收集器。
     * @param cube    该面所属的 cube，用于判定扁平修正条件。
     * @param quad    待提交的面，调用方已确认非 {@code null}。
     * @param red     颜色红色分量。
     * @param green   颜色绿色分量。
     * @param blue    颜色蓝色分量。
     * @param alpha   颜色透明度分量。
     */
    private void submitQuad(BufferBuilder builder, GeoCube cube, GeoQuad quad,
                            float red, float green, float blue, float alpha) {
        Vector3f normal = new Vector3f(quad.normal.getX(), quad.normal.getY(), quad.normal.getZ());
        MATRIX_STACK.getNormalMatrix().transform(normal);

        // 扁平 cube 的暗面着色修正，并兼容 Optifine shader：某一维厚度为 0 时，
        // 该维之外的法线分量若为负会导致该面整体发黑，故按维度条件翻转。
        // 三个条件与 GeckoLib 原实现逐字一致，遗漏会出现局部发黑。
        if ((cube.size.y == 0 || cube.size.z == 0) && normal.getX() < 0) {
            normal.x *= -1;
        }
        if ((cube.size.x == 0 || cube.size.z == 0) && normal.getY() < 0) {
            normal.y *= -1;
        }
        if ((cube.size.x == 0 || cube.size.y == 0) && normal.getZ() < 0) {
            normal.z *= -1;
        }

        for (GeoVertex vertex : quad.vertices) {
            Vector4f position = new Vector4f(
                    vertex.position.getX(), vertex.position.getY(), vertex.position.getZ(), 1.0F);
            MATRIX_STACK.getModelMatrix().transform(position);

            builder.pos(position.getX(), position.getY(), position.getZ())
                    .tex(vertex.textureU, vertex.textureV)
                    .color(red, green, blue, alpha)
                    .normal(normal.getX(), normal.getY(), normal.getZ())
                    .endVertex();
        }
    }

    /**
     * 提供模型提供器，供 {@link IGeoRenderer} 的默认实现使用。
     *
     * <p>返回类型沿用 GeckoLib 声明的原始类型，与其接口签名保持一致。
     *
     * @return 本渲染器绑定的模型提供器。
     */
    @SuppressWarnings("rawtypes")
    @Override
    public GeoModelProvider getGeoModelProvider() {
        return modelProvider;
    }

    /**
     * 提供实体贴图位置，供 {@link IGeoRenderer} 使用。
     *
     * @param instance 待渲染实体。
     * @return 贴图资源位置。
     */
    @Override
    public ResourceLocation getTextureLocation(FloatingPodEntity instance) {
        return modelProvider.getTextureLocation(instance);
    }

    /**
     * 提供实体贴图位置，供 vanilla {@link Render} 使用。
     *
     * @param entity 待渲染实体。
     * @return 贴图资源位置。
     */
    @Override
    protected ResourceLocation getEntityTexture(FloatingPodEntity entity) {
        return modelProvider.getTextureLocation(entity);
    }
}
