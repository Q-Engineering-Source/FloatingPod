package com.wi.floatingpod.server.source;

import com.wi.floatingpod.config.SourceLimitProfile;
import com.wi.floatingpod.server.source.SourceForestMembershipCodec.ForestExpectation;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.util.ArrayList;
import static org.junit.jupiter.api.Assertions.*;

class SourceForestCarrierSummaryTest {
    private static final class Budget implements SourceForestAllocationBudget {
        final SourceLimitProfile profile = SourceForestFixtures.LIMITS;
        long reserved;
        IOException cleanup;
        @Override public SourceLimitProfile exactProfile() { return profile; }
        @Override public void reserveTemporary(long bytes, String context) { reserved = Math.addExact(reserved, bytes); }
        @Override public void releaseTemporary(long bytes, String context) throws IOException {
            if (cleanup != null) throw cleanup;
            reserved = Math.subtractExact(reserved, bytes);
        }
    }
    private ForestExpectation expectation(SourceForestCodec.Encoded encoded) {
        var id = encoded.data().identity();
        return new ForestExpectation(id.bindingId(), id.unitId(), id.forestId(), id.sourceEpoch(), id.createdOperationId(),
            SourceForestCodec.DOMAIN, encoded.memberCount(), encoded.rootCount(), encoded.carrierCount(), encoded.retainedBytes());
    }
    @Test void completeDecoderYieldsCoordinatesAndReleasesActualBudget() throws Exception {
        var budget = new Budget();
        var encoded = SourceForestCodec.encode(SourceForestFixtures.forest(), budget.profile);
        var summaries = new ArrayList<SourceForestCarrierSummary.CarrierData>();
        SourceForestCarrierSummary.consumeVerified(encoded.bytes().copy(), encoded.sha256().copy(), expectation(encoded),
            budget.profile, budget, summaries::add);
        assertEquals(2, summaries.size());
        assertEquals(0, summaries.getFirst().dimension());
        assertEquals(32, summaries.get(1).chunkX());
        assertEquals("cleanroom/scalar", summaries.getFirst().identity().loaderProfile());
        assertEquals(0, budget.reserved);
    }
    @Test void mismatchedSqlCountsFailBeforeAnyConsumerAndReleaseBudget() throws Exception {
        var budget = new Budget();
        var encoded = SourceForestCodec.encode(SourceForestFixtures.forest(), budget.profile);
        var correct = expectation(encoded);
        var wrong = new ForestExpectation(correct.binding(), correct.unit(), correct.forest(), correct.sourceEpoch(),
            correct.createdOperation(), correct.codec(), 999, correct.rootCount(), correct.carrierCount(), correct.retainedBytes());
        assertThrows(IllegalArgumentException.class, () -> SourceForestCarrierSummary.consumeVerified(encoded.bytes().copy(),
            encoded.sha256().copy(), wrong, budget.profile, budget, summary -> fail("consumer before complete verification")));
        assertEquals(0, budget.reserved);
    }
    @Test void consumerFailureRetainsOriginalAndCleanupAsSuppressed() throws Exception {
        var budget = new Budget();
        var encoded = SourceForestCodec.encode(SourceForestFixtures.forest(), budget.profile);
        var original = new IllegalStateException("consumer fault");
        var cleanup = new IOException("budget cleanup fault");
        Throwable actual = assertThrows(IllegalStateException.class, () -> SourceForestCarrierSummary.consumeVerified(
            encoded.bytes().copy(), encoded.sha256().copy(), expectation(encoded), budget.profile, budget, summary -> {
                budget.cleanup = cleanup;
                throw original;
            }));
        assertSame(original, actual);
        assertSame(cleanup, actual.getSuppressed()[0].getCause());
    }
}
