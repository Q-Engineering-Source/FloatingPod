package com.wi.floatingpod.debug;

import com.wi.floatingpod.entity.FloatingPodEntity;
import com.wi.floatingpod.item.FloatingPodItems;
import net.minecraft.entity.player.EntityPlayer;
import net.minecraft.entity.player.EntityPlayerMP;
import net.minecraft.item.ItemStack;
import net.minecraft.util.EnumActionResult;
import net.minecraft.util.EnumFacing;
import net.minecraft.util.EnumHand;
import net.minecraft.util.math.AxisAlignedBB;
import net.minecraft.util.math.BlockPos;
import net.minecraft.util.text.TextComponentTranslation;
import net.minecraft.world.World;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Vanilla packet 已排到 Main；客户端只消费 gesture，不提交任何业务或实体写入。 */
public final class DebugItemInteractions {
    private static final Logger LOGGER = LogManager.getLogger("floatingpod");
    private DebugItemInteractions() { }

    public static boolean holdsTool(EntityPlayer player, EnumHand hand) {
        ItemStack stack = player.getHeldItem(hand);
        return !stack.isEmpty() && stack.getItem() == FloatingPodItems.POD_DEBUG_TOOL;
    }

    static DebugItemDispatch.Hand dispatchHand(EnumHand hand) {
        if (hand == null) throw new IllegalArgumentException("Actual Minecraft hand required");
        return hand == EnumHand.MAIN_HAND
                ? DebugItemDispatch.Hand.MAIN_HAND : DebugItemDispatch.Hand.OFF_HAND;
    }

    static EnumHand minecraftHand(DebugItemDispatch.Hand hand) {
        if (hand == null) throw new IllegalArgumentException("Retained debug hand required");
        return switch (hand) {
            case MAIN_HAND -> EnumHand.MAIN_HAND;
            case OFF_HAND -> EnumHand.OFF_HAND;
        };
    }

    public static EnumActionResult handleBlockUse(EntityPlayer player, World world, BlockPos clicked,
                                                   EnumFacing face, EnumHand hand) {
        try {
            return blockUse(player, world, clicked, face, hand);
        } catch (RuntimeException failure) {
            LOGGER.error("FloatingPod debug item placement validation failed", failure);
            if (player instanceof EntityPlayerMP actual)
                feedback(actual, DebugItemEndpoint.Reply.of(DebugItemEndpoint.Status.FAULTED));
            return EnumActionResult.FAIL;
        }
    }

    private static EnumActionResult blockUse(EntityPlayer player, World world, BlockPos clicked,
                                              EnumFacing face, EnumHand hand) {
        if (!holdsTool(player, hand)) return EnumActionResult.PASS;
        if (world.isRemote) return EnumActionResult.SUCCESS;
        if (!(player instanceof EntityPlayerMP actual)) return EnumActionResult.FAIL;
        PodDebugRuntimeRegistry.requireMain(actual.getServer());
        DebugItemEndpoint.PlacementFace originalFace = switch(face) {
            case DOWN -> DebugItemEndpoint.PlacementFace.DOWN; case UP -> DebugItemEndpoint.PlacementFace.UP;
            case NORTH -> DebugItemEndpoint.PlacementFace.NORTH; case SOUTH -> DebugItemEndpoint.PlacementFace.SOUTH;
            case WEST -> DebugItemEndpoint.PlacementFace.WEST; case EAST -> DebugItemEndpoint.PlacementFace.EAST;
        };
        DebugItemEndpoint.PlacementContext context = new DebugItemEndpoint.PlacementContext(
                clicked.getX(), clicked.getY(), clicked.getZ(), originalFace);
        BlockPos target = new BlockPos(context.targetX(), context.targetY(), context.targetZ());
        double x = target.getX() + 0.5, y = target.getY(), z = target.getZ() + 0.5;
        double reach = actual.interactionManager.getBlockReachDistance();
        boolean reachable = actual.getDistanceSq(clicked.getX() + 0.5, clicked.getY() + 0.5,
                clicked.getZ() + 0.5) <= reach * reach;
        AxisAlignedBB body = new AxisAlignedBB(x - 0.5, y, z - 0.5, x + 0.5, y + 1.0, z + 0.5);
        ItemStack held = actual.getHeldItem(hand);
        boolean allowed = target.getY() > 0 && Math.addExact(target.getY(), 1) < world.getHeight()
                && world.isBlockLoaded(target) && world.isBlockLoaded(target.down()) && world.isBlockLoaded(target.up())
                && body.minX >= world.getWorldBorder().minX() && body.maxX <= world.getWorldBorder().maxX()
                && body.minZ >= world.getWorldBorder().minZ() && body.maxZ <= world.getWorldBorder().maxZ()
                && world.isBlockModifiable(actual, clicked) && world.isBlockModifiable(actual, target)
                && world.isBlockModifiable(actual, target.up())
                && actual.canPlayerEdit(clicked, face, held)
                && actual.canPlayerEdit(target, face, held)
                && actual.canPlayerEdit(target.up(), face, held)
                && world.getBlockState(target.down()).isSideSolid(world, target.down(), EnumFacing.UP)
                && world.getCollisionBoxes(null, body).isEmpty()
                && !world.containsAnyLiquid(body)
                && world.getEntitiesWithinAABBExcludingEntity(null, body).isEmpty();
        DebugItemDispatch.Result result = execute(actual, world, null, hand, DebugItemEndpoint.Action.PLACE,
                new DebugItemEndpoint.Position(x, y, z, actual.rotationYaw), context, reachable, allowed,
                actual.canUseCommand(2, "floatingpod.debug.place"));
        if (!result.duplicate()) feedback(actual, result.reply());
        // Pending 从来不是 committed success。
        return EnumActionResult.FAIL;
    }

    public static boolean handlePodInteract(FloatingPodEntity pod, EntityPlayer player, EnumHand hand) {
        try {
            return podInteract(pod, player, hand);
        } catch (RuntimeException failure) {
            LOGGER.error("FloatingPod debug item entity interaction validation failed", failure);
            if (player instanceof EntityPlayerMP actual)
                feedback(actual, DebugItemEndpoint.Reply.of(DebugItemEndpoint.Status.FAULTED));
            return true;
        }
    }

    private static boolean podInteract(FloatingPodEntity pod, EntityPlayer player, EnumHand hand) {
        if (!holdsTool(player, hand)) return false;
        if (pod.world.isRemote) return true;
        if (!(player instanceof EntityPlayerMP actual)) return true;
        PodDebugRuntimeRegistry.requireMain(actual.getServer());
        double reach = actual.canEntityBeSeen(pod) ? 6.0 : 3.0;
        boolean reachable = actual.getDistanceSq(pod) < reach * reach;
        DebugItemEndpoint.Action action = actual.isSneaking()
                ? DebugItemEndpoint.Action.TOGGLE : DebugItemEndpoint.Action.INSPECT;
        DebugItemDispatch.Result result = execute(actual, pod.world, pod, hand, action,
                null, null, reachable, true, false);
        if (!result.duplicate()) feedback(actual, result.reply());
        return true;
    }

    private static DebugItemDispatch.Result execute(EntityPlayerMP player, World world,
            FloatingPodEntity pod, EnumHand hand, DebugItemEndpoint.Action action,
            DebugItemEndpoint.Position position, DebugItemEndpoint.PlacementContext context,
            boolean reachable, boolean protectedPlacement, boolean administrator) {
        DebugItemDispatch.Hand actualHand = dispatchHand(hand);
        boolean actualSession = PodDebugRuntimeRegistry.actualPlayer(player.getServer(), player, world);
        if (!actualSession)
            return new DebugItemDispatch.Result(null,
                    DebugItemEndpoint.Reply.of(DebugItemEndpoint.Status.REJECTED), false);
        // 在生成新 Request 或进入 endpoint 前拒绝；重复点击只反馈原操作仍在处理。
        if (pod != null && pod.world == world && !pod.acceptsBusinessInteraction())
            return new DebugItemDispatch.Result(null, DebugItemEndpoint.Reply.of(
                    pod.hasPendingBusinessOperation() ? DebugItemEndpoint.Status.PENDING
                            : DebugItemEndpoint.Status.REJECTED), false);
        boolean exact = pod == null || (pod.world == world && pod.acceptsBusinessInteraction()
                && world.getEntityByID(pod.getEntityId()) == pod);

        PodDebugRuntimeRegistry.Lease lease = PodDebugRuntimeRegistry.current(player.getServer());
        if (lease == null)
            return new DebugItemDispatch.Result(null,
                    DebugItemEndpoint.Reply.of(DebugItemEndpoint.Status.UNAVAILABLE), false);
        DebugItemEndpoint.Identity identity = PodDebugRuntimeRegistry.identity(lease, player, world, pod);
        long tick = player.getServer().getTickCounter();
        DebugItemDispatch dispatch = PodDebugRuntimeRegistry.dispatch();
        try {
            DebugItemDispatch.Facts facts = new DebugItemDispatch.Facts(true, actualSession, actualHand,
                    holdsTool(player, hand), exact, reachable, protectedPlacement, administrator);
            return dispatch.handleActual(lease.endpoint(), facts, identity, actualHand, tick, action, position, context);
        } catch (RuntimeException failure) {
            DebugItemEndpoint.Request request = dispatch.findRequest(identity, action, position, context, tick);
            String operation = request == null ? "" : " operation=" + request.operation();
            LOGGER.error("FloatingPod debug item failed: action=" + action + operation, failure);
            return new DebugItemDispatch.Result(request,
                    DebugItemEndpoint.Reply.of(DebugItemEndpoint.Status.FAULTED), false);
        }
    }

    static void feedback(EntityPlayerMP player, DebugItemEndpoint.Reply reply) {
        if (reply.status() == DebugItemEndpoint.Status.INSPECTED) {
            DebugItemEndpoint.View view = reply.view();
            DebugItemFeedback.Message message = DebugItemFeedback.state(view);
            Object[] parameters = message.parameters().stream().map(value ->
                    value instanceof DebugItemFeedback.Translation translation
                            ? new TextComponentTranslation(translation.key()) : value).toArray();
            player.sendStatusMessage(new TextComponentTranslation(message.key(), parameters), false);
        } else {
            player.sendStatusMessage(new TextComponentTranslation("floatingpod.debug_tool."
                    + reply.status().name().toLowerCase(java.util.Locale.ROOT)), false);
        }
    }
}
