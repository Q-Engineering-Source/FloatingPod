package com.wi.floatingpod.client;

import com.wi.floatingpod.SidedSetup;
import com.wi.floatingpod.client.render.BloomFrameBoundary;
import com.wi.floatingpod.client.render.BloomPainterBinding;
import com.wi.floatingpod.client.render.BloomQueue;
import com.wi.floatingpod.client.render.FloatingPodModel;
import com.wi.floatingpod.client.render.FloatingPodRenderer;
import com.wi.floatingpod.client.render.LumenizedBloomRegistrar;
import com.wi.floatingpod.entity.FloatingPodEntity;
import net.minecraftforge.common.MinecraftForge;
import net.minecraftforge.fml.client.registry.IRenderFactory;
import net.minecraftforge.fml.client.registry.RenderingRegistry;
import net.minecraftforge.fml.common.Loader;
import net.minecraftforge.fml.relauncher.Side;
import net.minecraftforge.fml.relauncher.SideOnly;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.wi.floatingpod.vendored.geckolib3.GeckoLib;
import com.wi.floatingpod.vendored.geckolib3.core.IAnimatable;
import com.wi.floatingpod.vendored.geckolib3.core.IAnimatableModel;
import com.wi.floatingpod.vendored.geckolib3.core.controller.AnimationController;

/**
 * {@link SidedSetup} 的客户端实现，集中承担全部客户端渲染装配。
 *
 * <p>本类及其引用的模型与渲染器只在客户端加载。专用服务端由 FML 的 {@code @SidedProxy}
 * 装配另一个实现，因此不会解析本类，也不会牵出 Gecko 客户端类。
 */
@SideOnly(Side.CLIENT)
public class ClientSetupImpl implements SidedSetup {

    /**
     * 本实体类型共用的模型提供器。
     *
     * <p>必须是单一实例：模型提供器内部持有 {@code AnimationProcessor} 与已登记的 bone，
     * 渲染器与 model fetcher 必须看到同一个实例，动画推进结果才能作用于实际绘制的骨架。
     * GeckoLib 自身也是每种实体一个提供器实例。
     */
    private final FloatingPodModel modelProvider = new FloatingPodModel();

    /** Lumenized 的 mod id，用于判定其是否加载。 */
    private static final String LUMENIZED_MOD_ID = "lumenized";

    /** 记录辉光注册结果的日志入口。 */
    private static final Logger LOGGER = LogManager.getLogger("floatingpod");

    /**
     * 本帧辉光请求队列；Lumenized 未加载时为 {@code null}。
     *
     * <p>为 {@code null} 时渲染器不入队，也不存在消费方，因此整条辉光路径不被触达。
     */
    private BloomQueue bloomQueue;

    /**
     * 辉光绘制者绑定入口；Lumenized 未加载时为 {@code null}。
     */
    private BloomPainterBinding bloomBinding;

    /**
     * 供 FML 的 {@code @SidedProxy} 反射实例化。
     */
    public ClientSetupImpl() {
    }

    /**
     * 执行客户端装配：初始化 GeckoLib、注册 model fetcher、按需注册辉光、绑定实体渲染器。
     *
     * <p>各步都必须发生在客户端预初始化阶段：GeckoLib 的资源重载监听器需要在资源管理器
     * 首次加载前注册，实体渲染器需要在实体首次进入视野前绑定，辉光 ticket 需要在
     * 首次世界渲染前进入 Lumenized 的注册表。
     *
     * <p>辉光注册必须排在渲染器注册之前：渲染器构造时会把自身绑定到
     * {@link BloomPainterBinding}，而该实例只在辉光注册这一步创建。
     */
    @Override
    public void onPreInitialization() {
        GeckoLib.initialize();
        registerModelFetcher();
        registerBloomIfAvailable();
        registerEntityRenderer();
    }

    /**
     * 仅在 Lumenized 已加载时注册辉光叠加路径。
     *
     * <p><b>类加载隔离</b>：Lumenized 是 {@code doc/01-technical-baseline.md} 规定的客户端侧
     * 可选前置，以 {@code compileOnly} 编译、不进产物、执行期不保证存在。
     * 引用其类型的全部代码集中在 {@code LumenizedBloomBridgeImpl} 与
     * {@code LumenizedBloomRegistrar} 两个类中，本方法是通往它们的唯一入口，
     * 且被 {@link Loader#isModLoaded} 守卫。判定为假时二者都不被解析，
     * 因此不会抛 {@code NoClassDefFoundError}。本类自身不引用任何 {@code gregtech} 类型，
     * 只经中立的队列、绑定与帧边界类型交互。
     *
     * <p>Lumenized 缺失时不注册辉光、只保留伪发光，这不是静默降级，而是
     * {@code doc/01-technical-baseline.md} 明确规定的行为。
     *
     * <p>另一种不注册的情形是 Lumenized 自身拒绝：实测其 {@code registerBloomRender} 在
     * 检测到 Optifine shader pack 已加载时返回 {@code null}，因为其辉光后处理与 shader pack
     * 不共存。此时伪发光基线同样继续生效，符合上述规定，但如实记录以便取证时区分
     * 「未注册」与「已注册但未生效」。
     *
     * <p>只有注册成功才写入两个字段并注册帧边界：字段为 {@code null} 时渲染器不入队，
     * 因此注册失败时不会积累无人消费的请求。帧边界负责在每帧开始前清空上一帧残留，
     * 它必须与队列同生命周期，故在此一并注册，见 {@link BloomFrameBoundary}。
     */
    private void registerBloomIfAvailable() {
        if (!Loader.isModLoaded(LUMENIZED_MOD_ID)) {
            LOGGER.info("FloatingPod 未检测到 Lumenized，bloom bone 仅呈现伪发光效果，不叠加辉光");
            return;
        }
        BloomQueue queue = new BloomQueue();
        BloomPainterBinding binding = new BloomPainterBinding();
        if (!LumenizedBloomRegistrar.register(queue, binding)) {
            LOGGER.info("FloatingPod 的辉光注册被 Lumenized 拒绝（通常因已加载 Optifine shader pack），"
                    + "bloom bone 仅呈现伪发光效果");
            return;
        }
        bloomQueue = queue;
        bloomBinding = binding;
        MinecraftForge.EVENT_BUS.register(new BloomFrameBoundary(queue));
        LOGGER.info("FloatingPod 已向 Lumenized 注册辉光叠加路径");
    }

    /**
     * 注册把 {@link IAnimatable} 映射回其模型的 model fetcher。
     *
     * <p>存在动机：GeckoLib 通常由 {@code GeoEntityRenderer} 的静态初始化块完成该注册，
     * 但本项目的渲染器不继承该类（其类型上界要求 {@code EntityLivingBase}，不适用于非生物的
     * 浮游辅助单元）。缺少该注册时 {@code AnimationController} 找不到模型，
     * 会走到「Could not find suitable model」分支并返回 {@code null}，动画不会运行。
     * 因此在此显式注册一个只认本实体类型的等价实现。
     */
    private void registerModelFetcher() {
        AnimationController.addModelFetcher((IAnimatable animatable) -> {
            if (animatable instanceof FloatingPodEntity) {
                return asUntypedModel(modelProvider);
            }
            return null;
        });
    }

    /**
     * 绑定实体渲染器工厂。工厂在客户端首次需要渲染该实体时被调用，
     * 并复用本类持有的同一个模型提供器实例。
     */
    private void registerEntityRenderer() {
        RenderingRegistry.registerEntityRenderingHandler(
                FloatingPodEntity.class,
                (IRenderFactory<FloatingPodEntity>) renderManager ->
                        new FloatingPodRenderer(renderManager, modelProvider, bloomQueue, bloomBinding));
    }

    /**
     * 把强类型模型适配为 GeckoLib model fetcher 所需的无类型形式。
     *
     * <p>GeckoLib 的 {@code ModelFetcher} 以 {@code IAnimatableModel<Object>} 交换模型，
     * 而本项目的模型是 {@code IAnimatableModel<FloatingPodEntity>}。该转换在此集中一处完成，
     * 并且只在确认 animatable 确实是 {@link FloatingPodEntity} 之后调用，因此运行期类型是相符的。
     *
     * @param model 本实体类型的模型提供器。
     * @return 同一个实例的无类型视图。
     */
    @SuppressWarnings("unchecked")
    private static IAnimatableModel<Object> asUntypedModel(IAnimatableModel<FloatingPodEntity> model) {
        return (IAnimatableModel<Object>) (IAnimatableModel<?>) model;
    }
}
