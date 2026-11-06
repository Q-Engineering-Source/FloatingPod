package com.wi.floatingpod.entity;

import java.util.UUID;

/** 服务端实体控制态不可变值；客户端只接收从该值派生的 FROZEN 镜像。 */
public final class FloatingPodControlSnapshot {
    public enum Phase { PENDING_INITIALIZATION, COMMITTED, FAULTED }

    private final Phase phase;
    private final UUID podUuid;
    private final UUID ownerUuid;
    private final String operatingMode;
    private final boolean active;
    private final long stateVersion;
    private final String worldName;
    private final double posX;
    private final double posY;
    private final double posZ;
    private final float yaw;
    private final float pitch;
    private final int dimensionId;
    private final UUID bindingUuid;
    private final UUID attemptUuid;
    private final UUID operationUuid;
    private final String faultType;

    public FloatingPodControlSnapshot(
            Phase phase,
            UUID podUuid,
            UUID ownerUuid,
            String operatingMode,
            boolean active,
            long stateVersion,
            String worldName,
            double posX,
            double posY,
            double posZ,
            float yaw,
            float pitch,
            int dimensionId,
            UUID bindingUuid,
            UUID attemptUuid,
            UUID operationUuid,
            String faultType) {
        if (phase == null || podUuid == null || operatingMode == null || worldName == null || worldName.trim().isEmpty()) {
            throw new IllegalArgumentException("FloatingPod control snapshot identity is incomplete");
        }
        if (!Double.isFinite(posX) || !Double.isFinite(posY) || !Double.isFinite(posZ)
                || !Float.isFinite(yaw) || !Float.isFinite(pitch) || stateVersion < 0L) {
            throw new IllegalArgumentException("FloatingPod control snapshot pose or version is invalid");
        }
        if (!"Normal".equals(operatingMode) && !"WorldIdle".equals(operatingMode)) {
            throw new IllegalArgumentException("Unknown FloatingPod operating mode: " + operatingMode);
        }
        if (active && ownerUuid == null && !"WorldIdle".equals(operatingMode)) {
            throw new IllegalArgumentException("Active unclaimed FloatingPod control must use WorldIdle");
        }
        if (phase == Phase.PENDING_INITIALIZATION && (active || stateVersion != 0L || ownerUuid != null || !"WorldIdle".equals(operatingMode))) {
            throw new IllegalArgumentException("Pending controlled creation must be inactive ownerless WorldIdle at version zero");
        }
        if (phase == Phase.COMMITTED && !active) {
            throw new IllegalArgumentException("Committed control snapshot must be active");
        }
        this.phase = phase;
        this.podUuid = podUuid;
        this.ownerUuid = ownerUuid;
        this.operatingMode = operatingMode;
        this.active = active;
        this.stateVersion = stateVersion;
        this.worldName = worldName;
        this.posX = posX;
        this.posY = posY;
        this.posZ = posZ;
        this.yaw = yaw;
        this.pitch = pitch;
        this.dimensionId = dimensionId;
        this.bindingUuid = bindingUuid;
        this.attemptUuid = attemptUuid;
        this.operationUuid = operationUuid;
        this.faultType = faultType;
    }

    public Phase phase() { return phase; }
    public UUID podUuid() { return podUuid; }
    public UUID ownerUuid() { return ownerUuid; }
    public String operatingMode() { return operatingMode; }
    public boolean active() { return active; }
    public long stateVersion() { return stateVersion; }
    public String worldName() { return worldName; }
    public double posX() { return posX; }
    public double posY() { return posY; }
    public double posZ() { return posZ; }
    public float yaw() { return yaw; }
    public float pitch() { return pitch; }
    public int dimensionId() { return dimensionId; }
    public UUID bindingUuid() { return bindingUuid; }
    public UUID attemptUuid() { return attemptUuid; }
    public UUID operationUuid() { return operationUuid; }
    public String faultType() { return faultType; }

    /** 未提交或 faulted 都 fail-closed；只有 committed Normal 才可移动。 */
    public boolean frozen() {
        return phase != Phase.COMMITTED || "WorldIdle".equals(operatingMode);
    }

    public FloatingPodControlSnapshot asFaulted(Throwable cause) {
        return new FloatingPodControlSnapshot(
                Phase.FAULTED, podUuid, ownerUuid, operatingMode, active, stateVersion, worldName,
                posX, posY, posZ, yaw, pitch, dimensionId, bindingUuid, attemptUuid, operationUuid,
                cause == null ? "unknown" : cause.getClass().getName());
    }
}
