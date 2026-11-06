package com.wi.floatingpod.debug;

import com.wi.floatingpod.entity.FloatingPodEntity;
import net.minecraft.entity.player.EntityPlayerMP;
import net.minecraft.item.ItemStack;
import net.minecraft.server.MinecraftServer;
import net.minecraft.util.EnumFacing;
import net.minecraft.util.EnumHand;
import net.minecraft.util.math.AxisAlignedBB;
import net.minecraft.util.math.BlockPos;
import net.minecraft.world.WorldServer;

/** 仅Main保留原连接/世界/body；不会进入owned worker command。 */
public final class MainDebugRequestContext implements MainPodRequestContext {
    private final DebugItemEndpoint.Request request;
    private final EntityPlayerMP player;
    private final Object connection;
    private final WorldServer world;
    private final FloatingPodEntity pod;
    private FloatingPodEntity createdBody;

    private MainDebugRequestContext(DebugItemEndpoint.Request request, EntityPlayerMP player,
                                    WorldServer world, FloatingPodEntity pod) {
        this.request = request; this.player = player; this.connection = player.connection;
        this.world = world; this.pod = pod;
        assertCurrent();
    }

    public static MainDebugRequestContext capture(MinecraftServer server, DebugItemEndpoint.Request request) {
        PodDebugRuntimeRegistry.requireMain(server);
        if (request == null) throw new IllegalArgumentException("Original request required");
        if (request.tick() != server.getTickCounter())
            throw new IllegalStateException("Only the current actual input tick may begin a new operation");
        EntityPlayerMP player = server.getPlayerList().getPlayerByUUID(request.identity().player());
        if (player == null || !(player.world instanceof WorldServer world))
            throw new IllegalStateException("Original live player/world unavailable");
        FloatingPodEntity pod = null;
        if (request.identity().pod() != null) {
            if (!(world.getEntityByID(request.identity().entityId()) instanceof FloatingPodEntity found))
                throw new IllegalStateException("Original body unavailable");
            pod = found;
        }
        return new MainDebugRequestContext(request, player, world, pod);
    }

    public void retainCreatedBody(FloatingPodEntity body) {
        PodDebugRuntimeRegistry.requireMain(player.getServer());
        if (request.action() != DebugItemEndpoint.Action.PLACE || createdBody != null || body == null
                || body.isDead || body.world != world)
            throw new IllegalStateException("Exact new body retention is one-shot");
        createdBody = body; // spawn callback前保留；不代表indexed/spawn/commit成功。
    }

    public WorldServer world() { assertCurrent(); return world; }
    public FloatingPodEntity pod() { assertCurrent(); return pod; }

    public void assertCurrent() {
        PodDebugRuntimeRegistry.requireMain(player.getServer());
        EnumHand actualHand = DebugItemInteractions.minecraftHand(request.hand());
        if (player.connection != connection || !PodDebugRuntimeRegistry.matchesCurrent(request, player, world, pod)
                || !DebugItemInteractions.holdsTool(player, actualHand))
            throw new IllegalStateException("Original connection/session/world/hand/tool changed");
        if (pod != null) {
            if (world.getEntityFromUuid(pod.getUniqueID()) != pod || pod.isDead || pod.world != world)
                throw new IllegalStateException("Original live UUID/body index changed");
            double reach = player.canEntityBeSeen(pod) ? 6.0 : 3.0;
            if (player.getDistanceSq(pod) >= reach * reach)
                throw new IllegalStateException("Original entity is outside actual reach");
        } else if (request.action() == DebugItemEndpoint.Action.PLACE) {
            assertPlacement();
        }
    }

    private void assertPlacement() {
        DebugItemEndpoint.PlacementContext context = request.placementContext();
        if (context == null || !DebugItemEndpoint.placementMatches(context, request.placement())
                || !player.canUseCommand(2, "floatingpod.debug.place"))
            throw new IllegalStateException("Original administrator placement context unavailable");
        BlockPos clicked = new BlockPos(context.clickedX(), context.clickedY(), context.clickedZ());
        BlockPos target = new BlockPos(context.targetX(), context.targetY(), context.targetZ());
        EnumFacing face = EnumFacing.valueOf(context.face().name());
        EnumHand actualHand = DebugItemInteractions.minecraftHand(request.hand());
        ItemStack held = player.getHeldItem(actualHand);
        double reach = player.interactionManager.getBlockReachDistance();
        double x = request.placement().x(), y = request.placement().y(), z = request.placement().z();
        AxisAlignedBB box = new AxisAlignedBB(x - .5, y, z - .5, x + .5, y + 1, z + .5);
        if (player.getDistanceSq(clicked.getX() + .5, clicked.getY() + .5, clicked.getZ() + .5) > reach * reach
                || target.getY() <= 0 || Math.addExact(target.getY(), 1) >= world.getHeight()
                || !world.isBlockLoaded(target) || !world.isBlockLoaded(target.down()) || !world.isBlockLoaded(target.up())
                || (box.minX < world.getWorldBorder().minX() || box.maxX > world.getWorldBorder().maxX()
                    || box.minZ < world.getWorldBorder().minZ() || box.maxZ > world.getWorldBorder().maxZ())
                || !world.isBlockModifiable(player, clicked) || !world.isBlockModifiable(player, target)
                || !world.isBlockModifiable(player, target.up())
                || !player.canPlayerEdit(clicked, face, held)
                || !player.canPlayerEdit(target, face, held)
                || !player.canPlayerEdit(target.up(), face, held)
                || !world.getBlockState(target.down()).isSideSolid(world, target.down(), EnumFacing.UP)
                || !world.getCollisionBoxes(createdBody, box).isEmpty() || world.containsAnyLiquid(box)
                || !world.getEntitiesWithinAABBExcludingEntity(createdBody, box).isEmpty())
            throw new IllegalStateException("Original placement is no longer reachable or protected/clear");
    }

    public void feedbackFailed() {
        PodDebugRuntimeRegistry.requireMain(player.getServer());
        if (player.connection != connection || !PodDebugRuntimeRegistry.matchesCurrent(request, player, world, pod))
            throw new IllegalStateException("Late failure feedback cannot target a replacement connection");
        DebugItemInteractions.feedback(player, DebugItemEndpoint.Reply.of(DebugItemEndpoint.Status.FAULTED));
    }

    /** PLACE成功后的body会占据placement box；ACK只核原连接/identity，不重新请求空体积。 */
    public void feedbackCommitted(DebugItemEndpoint.View view) {
        PodDebugRuntimeRegistry.requireMain(player.getServer());
        if (player.connection != connection || !PodDebugRuntimeRegistry.matchesCurrent(request, player, world, pod))
            throw new IllegalStateException("Late feedback cannot target a replacement connection");
        DebugItemInteractions.feedback(player, new DebugItemEndpoint.Reply(DebugItemEndpoint.Status.INSPECTED, view));
    }
}
