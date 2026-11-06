package com.wi.floatingpod.server.source;

import com.wi.floatingpod.config.SourceLimitProfile;
import com.wi.floatingpod.server.source.SourceForestCodec.*;
import com.wi.floatingpod.server.source.SourceForestMembershipCodec.ForestExpectation;
import java.io.IOException;
import java.util.Objects;
import java.util.function.Consumer;

/** 完整解码后的短生命周期 DATA；consumer 必须先在原账本预留其保留摘要。 */
public final class SourceForestCarrierSummary {
    private SourceForestCarrierSummary() { }
    public record CarrierData(Identity identity, Bytes sourceObservationSha, String regionIdentity, int dimension, int chunkX, int chunkZ,
                              long saveGeneration, CarrierDisposition disposition, boolean unresolvedMembers) { }

    public static void consumeVerified(byte[] bytes, byte[] sha, ForestExpectation expected,
            SourceLimitProfile limits, SourceForestAllocationBudget budget, Consumer<CarrierData> consumer) throws IOException {
        Objects.requireNonNull(expected); Objects.requireNonNull(consumer);
        try (var work = SourceForestAllocationBudget.Scope.open(budget, limits)) {
            Encoded decoded = SourceForestCodec.decodeWithin(bytes, sha, limits, work);
            Identity id = decoded.data().identity();
            SourceForestCodec.require(id.bindingId().equals(expected.binding()) && id.unitId().equals(expected.unit())
                && id.forestId().equals(expected.forest()) && id.sourceEpoch() == expected.sourceEpoch()
                && id.createdOperationId().equals(expected.createdOperation()) && SourceForestCodec.DOMAIN.equals(expected.codec())
                && decoded.memberCount() == expected.memberCount() && decoded.rootCount() == expected.rootCount()
                && decoded.carrierCount() == expected.carrierCount() && decoded.retainedBytes() == expected.retainedBytes(),
                "SQL forest identity/count/codec differs from complete carrier graph");
            work.charge(decoded.data().sourceObservation().length() + 256L, "source observation digest copy");
            Bytes observationSha = new Bytes(SourceForestCodec.hash(decoded.data().sourceObservation().copy(), work));
            for (Carrier carrier : decoded.data().carriers()) {
                boolean unresolved = decoded.data().members().stream().anyMatch(member ->
                    member.stages().stream().anyMatch(stage -> stage.carrierId().equals(carrier.id())
                        && (stage.classification() == Classification.UNKNOWN || member.disposition() == Disposition.PENDING
                        || member.disposition() == Disposition.FAULTED || member.disposition() == Disposition.UNKNOWN)));
                work.charge(128L, "temporary carrier summary");
                consumer.accept(new CarrierData(id, observationSha, carrier.regionKey(), carrier.dimension(), carrier.chunkX(), carrier.chunkZ(),
                    carrier.saveGeneration(), carrier.disposition(), unresolved));
            }
        }
    }
}
