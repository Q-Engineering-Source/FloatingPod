package com.wi.floatingpod.server;

import java.util.Objects;
import java.util.UUID;

/** Main 的实际后续 END 观察凭证；仅证明本体离开服务器索引，不证明 ticket 或磁盘清理。 */
public final class ExactBodyRemovalAcknowledgement {
    private final UUID bootId;
    private final long bootGeneration;
    private final long connectionGeneration;
    private final UUID operationId;
    private final UUID attemptId;
    private final UUID podId;
    private final long expectedStateVersion;
    private final int dimensionId;
    private final int entityId;
    private final long observedEndOrdinal;

    private ExactBodyRemovalAcknowledgement(UUID bootId, long bootGeneration,
            long connectionGeneration, UUID operationId, UUID attemptId, UUID podId,
            long expectedStateVersion, int dimensionId, int entityId, long observedEndOrdinal) {
        this.bootId = Objects.requireNonNull(bootId);
        this.operationId = Objects.requireNonNull(operationId);
        this.attemptId = Objects.requireNonNull(attemptId);
        this.podId = Objects.requireNonNull(podId);
        if (bootGeneration <= 0 || connectionGeneration <= 0 || expectedStateVersion < 0 || observedEndOrdinal < 0) {
            throw new IllegalArgumentException("Exact removal generations/version/END are invalid");
        }
        this.bootGeneration = bootGeneration;
        this.connectionGeneration = connectionGeneration;
        this.expectedStateVersion = expectedStateVersion;
        this.dimensionId = dimensionId;
        this.entityId = entityId;
        this.observedEndOrdinal = observedEndOrdinal;
    }

    // 只在 Main observer 从自身 pending 重新观察成功后调用；不能用公开 status DTO 构造。
    static ExactBodyRemovalAcknowledgement issue(UUID bootId, long bootGeneration,
            long connectionGeneration, UUID operationId, UUID attemptId, UUID podId,
            long expectedStateVersion, int dimensionId, int entityId, long observedEndOrdinal) {
        return new ExactBodyRemovalAcknowledgement(bootId, bootGeneration, connectionGeneration,
                operationId, attemptId, podId, expectedStateVersion, dimensionId, entityId, observedEndOrdinal);
    }

    public UUID bootId() { return bootId; }
    public long bootGeneration() { return bootGeneration; }
    public long connectionGeneration() { return connectionGeneration; }
    public UUID operationId() { return operationId; }
    public UUID attemptId() { return attemptId; }
    public UUID podId() { return podId; }
    public long expectedStateVersion() { return expectedStateVersion; }
    public int dimensionId() { return dimensionId; }
    public int entityId() { return entityId; }
    public long observedEndOrdinal() { return observedEndOrdinal; }
}
