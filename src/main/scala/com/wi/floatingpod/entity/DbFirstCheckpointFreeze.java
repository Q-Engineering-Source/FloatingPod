package com.wi.floatingpod.entity;

import java.util.Objects;
import java.util.UUID;

/** 完整active checkpoint的原Main冻结；既不取得收回权限，也不结束原normal-stop冻结。 */
public final class DbFirstCheckpointFreeze {
    private final Thread main;
    private final UUID operation;
    private final UUID normalStopRequest;
    private final FloatingPodControlSnapshot original;
    private final DbFirstPodSnapshot.Pose pose;

    public DbFirstCheckpointFreeze(UUID operation, long expectedVersion, FloatingPodControlSnapshot original,
            DbFirstPodSnapshot.Pose pose, UUID normalStopRequest) {
        this.operation = Objects.requireNonNull(operation, "Checkpoint operation");
        this.pose = Objects.requireNonNull(pose, "Exact frozen body pose");
        if (operation.equals(new UUID(0, 0)) || original == null
                || original.phase() != FloatingPodControlSnapshot.Phase.COMMITTED || !original.active()
                || original.faultType() != null || original.bindingUuid() == null || original.attemptUuid() == null
                || expectedVersion != original.stateVersion() || expectedVersion == Long.MAX_VALUE
                || !original.worldName().equals(pose.worldName()) || original.dimensionId() != pose.dimension()
                || centerChunk(original.posX()) != centerChunk(pose.x())
                || centerChunk(original.posZ()) != centerChunk(pose.z())) {
            throw new IllegalStateException("Checkpoint freeze requires exact healthy active control/version/world/authority");
        }
        this.original = original;
        this.normalStopRequest = normalStopRequest;
        this.main = Thread.currentThread();
    }

    /** 完整快照可保存同footprint内的真实漂移；跨中心区块须由独立ticket/locator迁移负责。 */
    private static int centerChunk(double coordinate) {
        double chunk = Math.floor(coordinate / 16.0D);
        if (!Double.isFinite(chunk) || chunk < (double) Integer.MIN_VALUE + 2 || chunk > (double) Integer.MAX_VALUE - 2)
            throw new IllegalStateException("Checkpoint chunk coordinate exceeds the managed footprint range");
        return (int) chunk;
    }

    public void requireCurrent(UUID operation, long expectedVersion, FloatingPodControlSnapshot current,
            DbFirstPodSnapshot.Pose actualPose) {
        if (Thread.currentThread() != main || !this.operation.equals(operation) || current != original
                || expectedVersion != original.stateVersion() || !pose.equals(actualPose)) {
            throw new IllegalStateException("Checkpoint freeze belongs to another Main/operation/control/version/pose");
        }
    }

    /** 纯控制态关系检查；调用方仍须独立消费原数据库确认，不能将此检查当成commit凭证。 */
    public void requireCommittedSuccessor(FloatingPodControlSnapshot successor) {
        if (Thread.currentThread() != main || successor == null
                || successor.phase() != FloatingPodControlSnapshot.Phase.COMMITTED || !successor.active()
                || successor.faultType() != null || !original.podUuid().equals(successor.podUuid())
                || !Objects.equals(original.ownerUuid(), successor.ownerUuid())
                || !original.operatingMode().equals(successor.operatingMode())
                || !original.bindingUuid().equals(successor.bindingUuid())
                || !original.attemptUuid().equals(successor.attemptUuid())
                || !Objects.equals(original.operationUuid(), successor.operationUuid())
                || successor.stateVersion() != Math.addExact(original.stateVersion(), 1L)
                || !pose.equals(new DbFirstPodSnapshot.Pose(successor.worldName(), successor.dimensionId(),
                    successor.posX(), successor.posY(), successor.posZ(), successor.yaw(), successor.pitch()))) {
            throw new IllegalStateException("Checkpoint successor changed business identity/mode/authority or frozen pose");
        }
    }

    public UUID operationUuid() { return operation; }
    public UUID normalStopRequest() { return normalStopRequest; }
    public FloatingPodControlSnapshot original() { return original; }
    public DbFirstPodSnapshot.Pose pose() { return pose; }
}
