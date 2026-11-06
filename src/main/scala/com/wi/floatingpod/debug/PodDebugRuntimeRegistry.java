package com.wi.floatingpod.debug;

import com.wi.floatingpod.FloatingPod;
import com.wi.floatingpod.entity.FloatingPodEntity;
import net.minecraft.entity.player.EntityPlayerMP;
import net.minecraft.server.MinecraftServer;
import net.minecraft.world.World;
import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.fml.common.eventhandler.SubscribeEvent;
import net.minecraftforge.fml.common.gameevent.PlayerEvent;

import java.util.UUID;
import java.util.WeakHashMap;

/** Ready 时安装，abort/stop 在释放业务资源之前按 exact lease 撤销。静态初始化不触达 DB。 */
@Mod.EventBusSubscriber(modid = FloatingPod.MOD_ID)
public final class PodDebugRuntimeRegistry {
    public record Lease(MinecraftServer server, UUID boot, DebugItemEndpoint endpoint) { }
    private static Lease installed;
    private static final WeakHashMap<Object, UUID> sessions = new WeakHashMap<>();
    private static final WeakHashMap<World, UUID> worlds = new WeakHashMap<>();
    private static final WeakHashMap<FloatingPodEntity, UUID> bodies = new WeakHashMap<>();
    private static final DebugItemDispatch dispatch = new DebugItemDispatch();
    private PodDebugRuntimeRegistry() { }

    public static Lease install(MinecraftServer server, UUID boot, DebugItemEndpoint endpoint) {
        requireMain(server);
        if (boot == null || endpoint == null || !endpoint.isReady())
            throw new IllegalStateException("Debug endpoint publication requires Ready boot");
        if (installed != null) throw new IllegalStateException("Debug endpoint is already installed");
        installed = new Lease(server, boot, endpoint);
        return installed;
    }

    public static void uninstall(Lease expected) {
        if (expected == null) return;
        requireMain(expected.server());
        if (installed != expected) return; // 旧 boot 不得撤销新 boot。
        installed = null;
        sessions.clear(); worlds.clear(); bodies.clear(); dispatch.clear();
    }

    public static Lease current(MinecraftServer server) {
        requireMain(server);
        return installed != null && installed.server() == server ? installed : null;
    }

    static DebugItemDispatch dispatch() { return dispatch; }

    static DebugItemEndpoint.Identity identity(Lease lease, EntityPlayerMP player, World world,
                                               FloatingPodEntity pod) {
        requireMain(lease.server());
        if (!actualPlayer(lease.server(), player, world))
            throw new IllegalStateException("Debug interaction session is no longer current");
        return new DebugItemEndpoint.Identity(lease.boot(), player.getUniqueID(),
                sessions.computeIfAbsent(player.connection, ignored -> UUID.randomUUID()),
                worlds.computeIfAbsent(world, ignored -> UUID.randomUUID()),
                world.getWorldInfo().getWorldName(), world.provider.getDimension(),
                pod == null ? -1 : pod.getEntityId(), pod == null ? null : pod.getUniqueID(),
                pod == null ? null : bodies.computeIfAbsent(pod, ignored -> UUID.randomUUID()));
    }

    /** 异步完成进入 Main 后再次核对；拒绝跨重登、换维度、换 body、重启后 ACK。 */
    public static boolean matchesCurrent(DebugItemEndpoint.Request request, EntityPlayerMP player,
                                         World world, FloatingPodEntity pod) {
        MinecraftServer server = player.getServer();
        requireMain(server);
        Lease lease = current(server);
        if (lease == null || !actualPlayer(server, player, world)) return false;
        if (pod != null && (pod.world != world || pod.isDead || world.getEntityByID(pod.getEntityId()) != pod))
            return false;
        return identity(lease, player, world, pod).equals(request.identity());
    }

    static boolean actualPlayer(MinecraftServer server, EntityPlayerMP player, World world) {
        return player.getServer() == server && player.world == world && world.getMinecraftServer() == server
                && player.connection != null && player.connection.netManager.isChannelOpen()
                && server.getPlayerList().getPlayerByUUID(player.getUniqueID()) == player && !player.isDead;
    }

    static void requireMain(MinecraftServer server) {
        if (server == null || !server.isCallingFromMinecraftThread())
            throw new IllegalStateException("Debug item runtime requires actual server Main thread");
    }

    @SubscribeEvent
    public static void onLogout(PlayerEvent.PlayerLoggedOutEvent event) {
        if (!(event.player instanceof EntityPlayerMP player)) return;
        requireMain(player.getServer());
        UUID session = sessions.remove(player.connection);
        if (session != null) dispatch.forget(session);
    }
}
