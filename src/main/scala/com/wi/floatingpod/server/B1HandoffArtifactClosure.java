package com.wi.floatingpod.server;

import com.wi.floatingpod.config.SourceLimitProfile;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** 原 V5 完整 legacy artifact closure DATA；body/hash/field 匹配不授予 native 或 full20 SQL 资格。 */
public final class B1HandoffArtifactClosure {
    public static final class Artifact {
        private final B1Full20WitnessReference reference;
        private final byte[] body;
        public Artifact(B1Full20WitnessReference reference, byte[] body) throws IOException {
            this.reference = Objects.requireNonNull(reference);
            if (body == null || body.length != reference.fullLength() ||
                !Arrays.equals(B1HandoffTransferDataCodec.digest(body), reference.fullSha256()))
                throw new IOException("Complete original witness body differs from actual reference DATA");
            this.body = body.clone();
        }
        public B1Full20WitnessReference reference() { return reference; }
        public byte[] body() { return body.clone(); }
    }
    private final List<Artifact> artifacts;
    private final List<B1Full20WitnessReference> references;
    /** 必须覆盖两 live V5 slots 的每个 distinct ref；显式历史 refs 也必须全身闭合。 */
    public B1HandoffArtifactClosure(SourceLimitProfile profile, byte[] sourceAnchor, byte[] receiptHash,
            B1ResumeRecordV5 inactive, B1ResumeRecordV5 current,
            List<B1Full20WitnessReference> retainedHistory, List<Artifact> artifacts) throws IOException {
        Objects.requireNonNull(profile); Objects.requireNonNull(inactive); Objects.requireNonNull(current);
        if (sourceAnchor == null || sourceAnchor.length != 32 || receiptHash == null || receiptHash.length != 32 ||
            retainedHistory == null || artifacts == null || artifacts.size() > profile.maxMembers() ||
            retainedHistory.size() > profile.maxMembers())
            throw new IOException("Exact bounded complete source/closure DATA required");
        Map<String, B1Full20WitnessReference> required = new LinkedHashMap<>();
        for (var r : inactive.allReferences()) merge(required, r);
        for (var r : current.allReferences()) merge(required, r);
        for (var r : retainedHistory) merge(required, r);
        if (required.size() != artifacts.size()) throw new IOException("Complete reference closure cardinality differs");
        Map<String, Artifact> supplied = new LinkedHashMap<>();
        long cumulative = 0;
        for (Artifact artifact : artifacts) {
            if (artifact == null) throw new IOException("Missing complete reference closure artifact");
            var r = artifact.reference;
            cumulative = Math.addExact(cumulative, r.fullLength());
            if (r.fullLength() > profile.maxNativeArtifactBytes() || cumulative > profile.maxRetainedBytes() ||
                !Arrays.equals(r.sourceAnchorSha256(), sourceAnchor) ||
                !Arrays.equals(r.originalReceiptSha256(), receiptHash) ||
                !Arrays.equals(r.profileSha256(), profile.sha256()) ||
                supplied.putIfAbsent(r.basename(), artifact) != null)
                throw new IOException("Closure source/profile/receipt/count/body bound differs");
            var expected = required.get(r.basename());
            if (expected == null || !expected.exactlyEquals(r))
                throw new IOException("Unreferenced/conflicting complete artifact supplied");
        }
        // 原 exact reference order 是 inactive→current→explicit retained history 首次出现顺序，禁止排序猜历史。
        this.artifacts = required.keySet().stream().map(supplied::get).toList();
        this.references = List.copyOf(required.values());
        Map<UUID, B1Full20WitnessReference> attempts = new LinkedHashMap<>();
        for (var r : this.references) {
            var previous = attempts.putIfAbsent(r.attemptId(), r);
            if (previous != null && !previous.exactlyEquals(r))
                throw new IOException("Same original attempt has conflicting full reference closure");
        }
    }
    private static void merge(Map<String, B1Full20WitnessReference> required,
            B1Full20WitnessReference reference) throws IOException {
        if (reference == null) throw new IOException("Missing declared historical reference");
        var previous = required.putIfAbsent(reference.basename(), reference);
        if (previous != null && !previous.exactlyEquals(reference))
            throw new IOException("Conflicting exact historical reference");
    }
    public List<Artifact> artifacts() { return artifacts; }
    public List<B1Full20WitnessReference> references() { return references; }
}
