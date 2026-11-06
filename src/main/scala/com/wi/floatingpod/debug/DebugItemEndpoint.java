package com.wi.floatingpod.debug;

import java.util.UUID;

/**
 * Main 线程上的非阻塞异步运行时入口。实现必须读实际 authority cache；不得调用同步 JDBC。
 * 提交只表示接纳 Pending，提交后确认由 future runtime 自己按原 session/operation 通知。
 * PlacementContext 与自有 Hand 仅保存 Main 原始输入用于校验/去重，不应交给 IO；IO 不得捕获 World、Entity、player 或 connection。
 */
public interface DebugItemEndpoint {
    enum Action { PLACE, INSPECT, TOGGLE }
    enum Status { PENDING, REJECTED, UNAVAILABLE, INSPECTED, FAULTED }
    /** 固定公共诊断码；禁止由原异常 message、binding 或路径生成展示原因。 */
    enum Reason { NONE, INITIALIZATION_PENDING, RECOVERY_PENDING, CLEANUP_PENDING, FAULTED }
    record Identity(UUID boot, UUID player, UUID session, UUID world, String worldName, int dimension,
                    int entityId, UUID pod, UUID body) {
        public Identity {
            if (boot == null || player == null || session == null || world == null || worldName == null
                    || worldName.isBlank() || ((pod == null) != (body == null))
                    || ((entityId == -1) != (pod == null)))
                throw new IllegalArgumentException("Incomplete debug Main identity");
        }
    }
    record Position(double x, double y, double z, float yaw) {
        public Position {
            if (!Double.isFinite(x) || !Double.isFinite(y) || !Double.isFinite(z) || !Float.isFinite(yaw))
                throw new IllegalArgumentException("Non-finite debug placement");
        }
    }
    /** 自有固定六方向，不将Minecraft对象或clicked block handle传给IO。 */
    enum PlacementFace {
        DOWN(0,-1,0), UP(0,1,0), NORTH(0,0,-1), SOUTH(0,0,1), WEST(-1,0,0), EAST(1,0,0);
        private final int dx, dy, dz;
        PlacementFace(int dx,int dy,int dz) { this.dx=dx; this.dy=dy; this.dz=dz; }
    }
    record PlacementContext(int clickedX, int clickedY, int clickedZ, PlacementFace face) {
        public PlacementContext {
            if(face==null)throw new IllegalArgumentException("Actual placement face required");
        }
        public int targetX() { return Math.addExact(clickedX,face.dx); }
        public int targetY() { return Math.addExact(clickedY,face.dy); }
        public int targetZ() { return Math.addExact(clickedZ,face.dz); }
    }
    /** 唯一纯target匹配逻辑：从原clicked/face派生，int溢出明确拒绝，不接受任意pose。 */
    static boolean placementMatches(PlacementContext context, Position position) {
        return context!=null && position!=null && position.x()==context.targetX()+0.5
                && position.y()==context.targetY() && position.z()==context.targetZ()+0.5;
    }
    record Request(Identity identity, long tick, UUID operation, Action action, Position placement,
                   PlacementContext placementContext, DebugItemDispatch.Hand hand) {
        /** 保留既有便利构造器作为明确的历史主手调用；实际回调必须传入真实手别。 */
        public Request(Identity identity,long tick,UUID operation,Action action,Position placement,
                       PlacementContext placementContext) {
            this(identity,tick,operation,action,placement,placementContext,DebugItemDispatch.Hand.MAIN_HAND);
        }
        /** 旧PLACE入口缺少原始gesture，仍由完整性检查明确拒绝。 */
        public Request(Identity identity,long tick,UUID operation,Action action,Position placement) {
            this(identity,tick,operation,action,placement,null,DebugItemDispatch.Hand.MAIN_HAND);
        }
        public Request {
            if (identity == null || operation == null || action == null || hand == null
                    || ((action == Action.PLACE) != (placement != null))
                    || ((action == Action.PLACE) != (identity.pod() == null))
                    || ((action == Action.PLACE) != (placementContext != null))
                    || (action == Action.PLACE && !placementMatches(placementContext,placement)))
                throw new IllegalArgumentException("Incomplete debug request");
        }
    }
    /** owner UUID/待认领属于冻结的最小公开调试状态；其他内部身份绝不展示。 */
    record CachedPod(Identity identity, UUID owner, String mode, boolean active, String phase, Reason reason) {
        public CachedPod {
            if (identity == null || reason == null || !("Normal".equals(mode) || "WorldIdle".equals(mode))
                    || !("Prepared".equals(phase) || "Ticketed".equals(phase)
                    || "PendingCleanup".equals(phase) || "PendingRecall".equals(phase)
                    || "Committed".equals(phase) || "Faulted".equals(phase)))
                throw new IllegalArgumentException("Invalid debug authority projection");
        }
    }
    record View(UUID pod, UUID owner, String mode, boolean active, String phase, Reason reason) {
        public View {
            if (pod == null || reason == null || !("Normal".equals(mode) || "WorldIdle".equals(mode))
                    || !("Prepared".equals(phase) || "Ticketed".equals(phase)
                    || "PendingCleanup".equals(phase) || "PendingRecall".equals(phase)
                    || "Committed".equals(phase) || "Faulted".equals(phase)))
                throw new IllegalArgumentException("Invalid public debug projection");
        }
    }
    record Reply(Status status, View view) {
        public Reply {
            if (status == null || (status == Status.INSPECTED) != (view != null))
                throw new IllegalArgumentException("Invalid debug reply");
        }
        public static Reply of(Status status) { return new Reply(status, null); }
    }

    /** 必须是真实 Ready，不能把尚未初始化的 stub 设为 Ready。 */
    boolean isReady();
    /** 无 IO、无创建/修复副作用；不存在或尚未接管返回 null。读故障保留异常。 */
    CachedPod inspect(Request request);
    /** 再核 exact boot/world/body/session；mutation 必须经异步 critical transaction seam。 */
    Reply submit(Request request);
}
