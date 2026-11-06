package com.wi.floatingpod.entity;

import java.util.Objects;
import com.wi.floatingpod.server.DbFirstBootActiveRecovery;
import java.util.UUID;

/** 单次 DB-first recall 的 Main 所有权；UNKNOWN 不释放此 token。 */
public final class DbFirstRecallFreeze {
    private final boolean bootRecovery;
    private final Thread main;
    private final UUID operation;
    private final FloatingPodControlSnapshot original;
    private final UUID normalStopRequest;
    private final NormalStopEntityFreeze normalStopOwner;
    private FloatingPodControlSnapshot retainedFault;
    private Throwable retainedCause;

    public DbFirstRecallFreeze(UUID operation, UUID authenticatedOwner, long expectedVersion,
                              FloatingPodControlSnapshot original) {
        this(operation, expectedVersion, requireAuthenticatedOwner(authenticatedOwner, original), null, null, false);
    }

    private DbFirstRecallFreeze(UUID operation, long expectedVersion, FloatingPodControlSnapshot original,
                               UUID normalStopRequest, NormalStopEntityFreeze normalStopOwner, boolean bootRecovery) {
        this.bootRecovery = bootRecovery;
        this.operation = Objects.requireNonNull(operation, "Recall operation");
        if (original == null || original.phase() != FloatingPodControlSnapshot.Phase.COMMITTED
                || !original.active() || original.faultType() != null || expectedVersion != original.stateVersion()
                || original.bindingUuid() == null || original.attemptUuid() == null)
            throw new IllegalStateException("Recall freeze requires exact healthy committed version/attempt/binding");
        this.original = original;
        this.main = Thread.currentThread();
        this.normalStopRequest = normalStopRequest;
        this.normalStopOwner = normalStopOwner;
    }

    private static FloatingPodControlSnapshot requireAuthenticatedOwner(UUID owner, FloatingPodControlSnapshot original) {
        Objects.requireNonNull(owner, "Authenticated recall owner");
        if (original == null || !owner.equals(original.ownerUuid()))
            throw new IllegalStateException("Recall freeze requires the authenticated original owner");
        return original;
    }

    /** 仅实体所在包用其私有原停服freeze建立；不是将持久owner UUID冒充玩家认证。 */
    static DbFirstRecallFreeze forNormalStop(UUID operation, UUID request, long expectedVersion,
            FloatingPodControlSnapshot original, NormalStopEntityFreeze originalStopOwner) {
        Objects.requireNonNull(request, "Original normal-stop request");
        Objects.requireNonNull(originalStopOwner, "Original normal-stop freeze").requireCurrent(request, original);
        if (!"Normal".equals(original.operatingMode()) || original.ownerUuid() == null)
            throw new IllegalStateException("Shutdown recall requires original owned Normal control");
        return new DbFirstRecallFreeze(operation, expectedVersion, original, request, originalStopOwner, false);
    }

    /** Boot不借玩家认证或停服来源；构造前回核真实owner私有调用期与已登记实体Entry。 */
    static DbFirstRecallFreeze forBoot(UUID operation, long expectedVersion,
            FloatingPodControlSnapshot original, DbFirstBootActiveRecovery boot, FloatingPodEntity entity) {
        Objects.requireNonNull(boot, "Actual Boot freeze owner")
                .requireBootEntityFreeze(entity, operation, expectedVersion, original);
        return new DbFirstRecallFreeze(operation, expectedVersion, original, null, null, true);
    }

    public boolean isBootRecovery() { return bootRecovery; }

    /** 普通收回不能借停服freeze，停服收回也不能替换/丢失实际实体持有者。 */
    void requireNormalStopOwner(NormalStopEntityFreeze actual) {
        if (Thread.currentThread() != main || actual != normalStopOwner)
            throw new IllegalStateException("Recall normal-stop freeze owner changed");
        if (actual != null) actual.requireCurrent(normalStopRequest, original);
    }

    public void requireCurrent(UUID operation, long expectedVersion, FloatingPodControlSnapshot current) {
        if (Thread.currentThread() != main || !this.operation.equals(operation)
                || expectedVersion != original.stateVersion() || current != original || retainedFault != null) {
            throw new IllegalStateException("Recall freeze belongs to another Main/operation/version/control snapshot");
        }
        if (normalStopOwner != null) normalStopOwner.requireCurrent(normalStopRequest, current);
    }

    /** 原实体登记故障控制态；不替换freeze，不恢复正常capture或清理资格。 */
    FloatingPodControlSnapshot retainFault(FloatingPodControlSnapshot current, Throwable cause) {
        if (retainedFault == null) {
            requireCurrent(operation, original.stateVersion(), current);
            FloatingPodControlSnapshot fault = current.asFaulted(cause);
            retainedCause = cause;
            retainedFault = fault;
        } else if (Thread.currentThread() != main || current != retainedFault) {
            throw new IllegalStateException("Recall fault belongs to another Main/control snapshot");
        }
        return retainedFault;
    }

    /** 仅核故障后的原体保管关系；调用方仍须另持原操作的数据库确认和清理权限。 */
    public void requireRetainedFailure(UUID operation, long expectedVersion,
                                       FloatingPodControlSnapshot current, Throwable cause) {
        if (Thread.currentThread() != main || !this.operation.equals(operation)
                || expectedVersion != original.stateVersion() || retainedFault == null
                || current != retainedFault || retainedCause == null || cause != retainedCause) {
            throw new IllegalStateException("Recall retained failure differs from original Main/operation/control/cause");
        }
        if (normalStopOwner != null) normalStopOwner.requireCurrent(normalStopRequest, original);
    }

    public UUID normalStopRequest() { return normalStopRequest; }
    public UUID operationUuid() { return operation; }
    public FloatingPodControlSnapshot original() { return original; }
}
