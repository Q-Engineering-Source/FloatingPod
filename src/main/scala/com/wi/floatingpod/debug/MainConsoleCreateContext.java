package com.wi.floatingpod.debug;

import com.wi.floatingpod.entity.FloatingPodEntity;
import net.minecraft.command.ICommandSender;
import net.minecraft.server.MinecraftServer;
import net.minecraft.util.math.AxisAlignedBB;
import net.minecraft.util.math.BlockPos;
import net.minecraft.util.text.TextComponentString;
import net.minecraft.world.WorldServer;
import java.util.UUID;

/** 本机控制台的真实 Main 保管。没有玩家会话，也不授予独立 SQL 或 pump 能力。 */
public final class MainConsoleCreateContext implements MainPodRequestContext {
    public static final class InvalidPlacement extends IllegalArgumentException {
        private InvalidPlacement(String message) { super(message); }
    }
    private final MinecraftServer server;
    private final PodDebugRuntimeRegistry.Lease lease;
    private final WorldServer world;
    private final MainPodOperationRequest.ConsoleCreate request;
    private FloatingPodEntity body;

    private MainConsoleCreateContext(MinecraftServer server, PodDebugRuntimeRegistry.Lease lease,
                                     WorldServer world, MainPodOperationRequest.ConsoleCreate request) {
        this.server=server; this.lease=lease; this.world=world; this.request=request;
        assertCurrent();
    }

    public static MainConsoleCreateContext capture(MinecraftServer server, ICommandSender sender,
            ConsolePodEndpoint endpoint, UUID operation, int dimension, DebugItemEndpoint.Position position) {
        PodDebugRuntimeRegistry.requireMain(server);
        if (sender != server) throw new IllegalArgumentException("Actual local console required");
        var lease=PodDebugRuntimeRegistry.current(server);
        if (lease == null || lease.endpoint() != endpoint)
            throw new IllegalStateException("Original Ready endpoint required");
        WorldServer world=server.getWorld(dimension);
        if (world == null) throw new InvalidPlacement("Loaded dimension required");
        if (position == null) {
            BlockPos spawn=world.getSpawnPoint();
            world.getChunkProvider().provideChunk(spawn.getX() >> 4,spawn.getZ() >> 4);
            BlockPos top=world.getTopSolidOrLiquidBlock(spawn);
            position=new DebugItemEndpoint.Position(spawn.getX()+.5, top.getY()+2, spawn.getZ()+.5, 0F);
        }
        return new MainConsoleCreateContext(server,lease,world,new MainPodOperationRequest.ConsoleCreate(
                lease.boot(),operation,world.getWorldInfo().getWorldName(),dimension,position));
    }

    public MainPodOperationRequest.ConsoleCreate request() { return request; }
    public WorldServer world() { assertCurrent(); return world; }
    private void assertLease() {
        PodDebugRuntimeRegistry.requireMain(server);
        if (PodDebugRuntimeRegistry.current(server) != lease || server.getWorld(request.dimension()) != world
                || world.getMinecraftServer() != server)
            throw new IllegalStateException("Original console boot/world changed");
    }
    public void assertCurrent() {
        assertLease();
        var p=request.placement();
        BlockPos target=new BlockPos(p.x(),p.y(),p.z());
        AxisAlignedBB box=new AxisAlignedBB(p.x()-.5,p.y(),p.z()-.5,p.x()+.5,p.y()+1,p.z()+.5);
        if (p.y() <= 0 || p.y()+1 >= world.getHeight()
                || !world.isBlockLoaded(target) || !world.isBlockLoaded(target.up())
                || box.minX < world.getWorldBorder().minX() || box.maxX > world.getWorldBorder().maxX()
                || box.minZ < world.getWorldBorder().minZ() || box.maxZ > world.getWorldBorder().maxZ()
                || !world.getCollisionBoxes(body,box).isEmpty() || world.containsAnyLiquid(box)
                || !world.getEntitiesWithinAABBExcludingEntity(body,box).isEmpty())
            throw new InvalidPlacement("Placement must be loaded, inside world bounds and clear");
    }
    public void retainCreatedBody(FloatingPodEntity created) {
        assertLease();
        if (body != null || created == null || created.isDead || created.world != world)
            throw new IllegalStateException("Exact new console body retention is one-shot");
        body=created;
    }
    public void feedbackCommitted(DebugItemEndpoint.View view) {
        assertLease();
        server.sendMessage(new TextComponentString("FP_POD_CREATE code=COMMITTED operation="+request.operation()
                +" pod="+view.pod()+" owner="+(view.owner()==null?"unbound":view.owner())
                +" mode="+view.mode()+" active="+view.active()+" phase="+view.phase()));
    }
    public void feedbackFailed() {
        assertLease();
        server.sendMessage(new TextComponentString("FP_POD_CREATE code=FAILED operation="+request.operation()));
    }
}
