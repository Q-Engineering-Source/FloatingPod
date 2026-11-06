package com.wi.floatingpod;

import com.wi.floatingpod.debug.FloatingPodCommand;

import com.wi.floatingpod.probe.ProbeInput;
import com.wi.floatingpod.probe.ScalaInteropProbe;
import com.wi.floatingpod.network.ForgeServerRuntimeFactory;
import com.wi.floatingpod.network.PacketHandler;
import com.wi.floatingpod.network.ServerRuntimeLifecycle;
import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.fml.common.SidedProxy;
import net.minecraftforge.fml.common.event.FMLPreInitializationEvent;
import net.minecraftforge.fml.common.event.FMLServerAboutToStartEvent;
import net.minecraftforge.fml.common.event.FMLServerStartingEvent;
import net.minecraftforge.fml.common.event.FMLServerStoppedEvent;
import net.minecraftforge.fml.common.event.FMLServerStoppingEvent;
import net.minecraftforge.fml.common.network.NetworkCheckHandler;
import net.minecraftforge.fml.relauncher.Side;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * FloatingPod 的 Mod 入口，负责在预初始化阶段验证 Scala 3 与 Java 25 的互操作边界。
 */
@Mod(
        modid = FloatingPod.MOD_ID,
        name = FloatingPod.MOD_NAME,
        version = FloatingPod.VERSION,
        dependencies = FloatingPod.DEPENDENCIES
)
public final class FloatingPod {

    /** Mod 的稳定标识，用于 FML 装载与探针输入。 */
    public static final String MOD_ID = "floatingpod";

    /** Mod 的用户可见名称，与资源元数据保持一致。 */
    public static final String MOD_NAME = "FloatingPod";

    /** 当前探针构建的版本标识。 */
    public static final String VERSION = "0.1.0-SNAPSHOT";

    /** FML 支持的 Scalar 运行时下限表达式，使用 Scalar mcmod.info 中的版本 3.8.4。 */
    public static final String SCALAR_DEPENDENCY = "required-after:scalar@[3.8.4,)";

    /** 当前服务器已核验的 JDBC 前置；升级须重新核对驱动产物与数据库行为。 */
    public static final String SQL_JDBC_DEPENDENCY = "required-after:sqljdbcdriverlib@[0.2.0-rc.4]";

    public static final String DEPENDENCIES = SCALAR_DEPENDENCY + ";" + SQL_JDBC_DEPENDENCY;

    /**
     * 按物理端装配的初始化入口。客户端注入渲染装配实现，服务端注入不含渲染引用的实现，
     * 使专用服务端在类加载层面就不会触达 Gecko 客户端类。
     */
    @SidedProxy(
            clientSide = "com.wi.floatingpod.client.ClientSetupImpl",
            serverSide = "com.wi.floatingpod.ServerSetupImpl"
    )
    public static SidedSetup sidedSetup;

    /** Scala 实现产生的可观察结果后缀，用于 Fail Fast 的行为断言。 */
    private static final String PROBE_SUFFIX = "@scala-3.8.4";

    /** 服务端可用的日志入口，用于记录探针成功或保留异常堆栈。 */
    private static final Logger LOGGER = LogManager.getLogger(MOD_ID);

    /** 仅含服务端运行时资源的生命周期协调器；客户端初始化不会参与其资源装配。 */
    private final ServerRuntimeLifecycle serverRuntimeLifecycle;

    /**
     * 创建由 FML 装载的 Mod 实例。
     */
    public FloatingPod() {
        this.serverRuntimeLifecycle = new ServerRuntimeLifecycle(new ForgeServerRuntimeFactory(this));
    }

    /**
     * 仅供同包直接测试替换服务端生命周期边界，避免以 reflection 触达 FML 入口内部状态。
     *
     * @param serverRuntimeLifecycle 已构造、可观察的服务端生命周期协调器。
     */
    FloatingPod(ServerRuntimeLifecycle serverRuntimeLifecycle) {
        if (serverRuntimeLifecycle == null) {
            throw new IllegalArgumentException("serverRuntimeLifecycle must not be null");
        }
        this.serverRuntimeLifecycle = serverRuntimeLifecycle;
    }

    /**
     * 在 FML 预初始化阶段直接调用 Scala 实现并记录互操作结果。
     *
     * @param event FML 提供的预初始化事件。
     */
    @Mod.EventHandler
    public void onPreInitialization(FMLPreInitializationEvent event) {
        String observation = verifyInterop(new ProbeInput(MOD_ID, 1));
        LOGGER.info("FloatingPod Stage 1-B Scala 3/Java 25 互操作探针通过: {}", observation);

        PacketHandler.initialize();
        sidedSetup.onPreInitialization();
    }

    /**
     * FML 通知服务器即将接受命令时，仅登记 ticket 恢复 callback。
     *
     * @param event FML 提供的服务器启动事件。
     */
    @Mod.EventHandler
    public void onServerAboutToStart(FMLServerAboutToStartEvent event) {
        serverRuntimeLifecycle.onServerAboutToStart(event);
    }

    /**
     * 在 ticket callback 已登记且世界可解析后恢复活动辅助机，再开放请求入口。
     *
     * @param event FML 提供的服务器启动事件。
     */
    @Mod.EventHandler
    public void onServerStarting(FMLServerStartingEvent event) {
        event.registerServerCommand(new FloatingPodCommand());
        serverRuntimeLifecycle.onServerStarting(event);
    }

    /**
     * FML 停服时先卸载请求入口，再停止刷盘并回收活动实体；repository 关闭留给 Stopped。
     *
     * @param event FML 提供的服务器停止事件。
     */
    @Mod.EventHandler
    public void onServerStopping(FMLServerStoppingEvent event) {
        try {
            serverRuntimeLifecycle.onServerStopping(event);
        } catch (Throwable failure) {
            LOGGER.error("FloatingPod 服务端停服协调失败", failure);
        }
    }

    /**
     * 所有世界与 ticket 工作均已结束后，执行一次 repository 关闭并缓存结果。
     *
     * @param event FML 提供的服务器已停止事件。
     */
    @Mod.EventHandler
    public void onServerStopped(FMLServerStoppedEvent event) {
        try {
            serverRuntimeLifecycle.onServerStopped(event);
        } catch (Throwable failure) {
            LOGGER.error("FloatingPod 服务端最终关闭失败", failure);
        }
    }

    /**
     * 将连接协商限制为远端声明的完全相同 FloatingPod 版本；缺失或不兼容版本一律拒绝。
     *
     * @param remoteVersions 远端在 FML 握手中声明的 mod 版本。
     * @param remoteSide 发起握手的远端物理端。
     * @return 仅当远端 FloatingPod 版本与本地 VERSION 完全相等时返回 true。
     */
    @NetworkCheckHandler
    public static boolean acceptsRemoteVersions(Map<String, String> remoteVersions, Side remoteSide) {
        return remoteVersions != null && VERSION.equals(remoteVersions.get(MOD_ID));
    }

    /**
     * 为 Gradle 验收 task 提供独立 JVM 入口，直接执行同一条 Java 到 Scala 调用路径。
     *
     * @param arguments 命令行参数；当前探针不接收额外参数。
     */
    public static void main(String[] arguments) {
        String observation = verifyInterop(new ProbeInput(MOD_ID, 1));
        System.out.println("FloatingPod Scala/Java interop probe: " + observation);
    }

    /**
     * 调用 Scala 实现并验证其结果，任何不一致都立即阻止启动或构建继续。
     *
     * @param input Java 侧构造的不可变探针输入。
     * @return 经验证的 Scala 计算结果。
     */
    private static String verifyInterop(ProbeInput input) {
        String observation = ScalaInteropProbe.evaluate(input);
        String expected = input.unitId() + "#" + input.sequence() + PROBE_SUFFIX;
        if (!expected.equals(observation)) {
            throw new IllegalStateException("Scala 3 互操作探针结果不匹配: " + observation);
        }
        return observation;
    }
}
