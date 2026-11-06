package com.wi.floatingpod.server;

import java.util.List;
import java.util.Objects;

/** 确切本体退场之后的 normal ticket 框架释放凭证；不表示 forcedchunks.dat 已保存。 */
public final class TicketReleaseAcknowledgement {
    public record Chunk(int dimensionId, int x, int z) {}

    private final ExactBodyRemovalAcknowledgement removal;
    private final List<Chunk> originalFootprint;

    private TicketReleaseAcknowledgement(ExactBodyRemovalAcknowledgement removal, List<Chunk> originalFootprint) {
        this.removal = Objects.requireNonNull(removal);
        this.originalFootprint = List.copyOf(originalFootprint);
        if (this.originalFootprint.isEmpty()
                || this.originalFootprint.stream().anyMatch(chunk -> chunk.dimensionId() != removal.dimensionId())
                || this.originalFootprint.stream().distinct().count() != this.originalFootprint.size()) {
            throw new IllegalArgumentException("Exact original ticket footprint required");
        }
    }

    static TicketReleaseAcknowledgement issue(ExactBodyRemovalAcknowledgement removal, List<Chunk> originalFootprint) {
        return new TicketReleaseAcknowledgement(removal, originalFootprint);
    }

    public ExactBodyRemovalAcknowledgement removal() { return removal; }
    public List<Chunk> originalFootprint() { return originalFootprint; }
}
