package com.wi.floatingpod.server;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static com.wi.floatingpod.server.B1HandoffTransferIntentFixture.*;

@Tag("b1-two-slot-pure")
class B1HandoffArtifactClosureTest {
    @Test void bothV5SlotReferencesDeduplicateOnlyExactSameFullReference() throws Exception {
        var p = parts();
        var closure = new B1HandoffArtifactClosure(PROFILE, p.bridge.sourceAnchorSha256(),
            B1HandoffTransferDataCodec.digest(p.bridge.originalReceipt()), p.inactive, p.current,
            List.of(p.witness), List.of(p.artifact));
        assertEquals(1, closure.references().size());
        assertTrue(p.witness.exactlyEquals(closure.references().get(0)));
        assertArrayEquals(p.artifact.body(), closure.artifacts().get(0).body());
    }
    @Test void fullBodyHashLengthAndClosureMissingOrExtraFail() throws Exception {
        var p = parts(); byte[] changed = p.artifact.body(); changed[0] ^= 1;
        assertThrows(IOException.class, () -> new B1HandoffArtifactClosure.Artifact(p.witness, changed));
        assertThrows(IOException.class, () -> new B1HandoffArtifactClosure.Artifact(p.witness, new byte[0]));
        assertThrows(IOException.class, () -> new B1HandoffArtifactClosure(PROFILE,
            p.bridge.sourceAnchorSha256(), B1HandoffTransferDataCodec.digest(p.bridge.originalReceipt()),
            p.inactive, p.current, List.of(), List.of()));
        assertThrows(IOException.class, () -> new B1HandoffArtifactClosure(PROFILE,
            p.bridge.sourceAnchorSha256(), B1HandoffTransferDataCodec.digest(p.bridge.originalReceipt()),
            p.inactive, p.current, List.of(), List.of(p.artifact, p.artifact)));
    }
    @Test void foreignProfileReceiptSourceAndConflictingHistoricalIdentityFail() throws Exception {
        var p = parts(); byte[] wrong = p.bridge.sourceAnchorSha256(); wrong[0] ^= 1;
        assertThrows(IOException.class, () -> new B1HandoffArtifactClosure(PROFILE, wrong,
            B1HandoffTransferDataCodec.digest(p.bridge.originalReceipt()), p.inactive, p.current, List.of(), List.of(p.artifact)));
        assertThrows(IOException.class, () -> new B1HandoffArtifactClosure(PROFILE, p.bridge.sourceAnchorSha256(),
            new byte[32], p.inactive, p.current, List.of(), List.of(p.artifact)));
        var foreignProfile = com.wi.floatingpod.config.SourceLimitProfile.of(
            10_000_000, 10_000_000, 10_000_000, 65, 100, 100_000_000, 10_000_000, 10_000_000);
        assertThrows(IOException.class, () -> new B1HandoffArtifactClosure(foreignProfile,
            p.bridge.sourceAnchorSha256(), B1HandoffTransferDataCodec.digest(p.bridge.originalReceipt()),
            p.inactive, p.current, List.of(), List.of(p.artifact)));
        var r = p.witness;
        var foreign = new B1Full20WitnessReference(r.basename(), id(99), r.fullLength(), r.fullSha256(),
            r.sourceAnchorSha256(), r.originalReceiptSha256(), r.profileSha256(), r.attemptId(), r.phase(), r.logicalOperation());
        assertThrows(IOException.class, () -> new B1HandoffArtifactClosure(PROFILE, p.bridge.sourceAnchorSha256(),
            B1HandoffTransferDataCodec.digest(p.bridge.originalReceipt()), p.inactive, p.current, List.of(foreign), List.of(p.artifact)));
    }
    @Test void returnedArtifactBodyDoesNotModifyRetainedOriginal() throws Exception {
        var p = parts(); byte[] original = p.artifact.body(); byte[] exposed = p.artifact.body(); exposed[0] ^= 1;
        assertArrayEquals(original, p.artifact.body());
    }
}
