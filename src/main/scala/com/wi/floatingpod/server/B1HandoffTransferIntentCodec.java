package com.wi.floatingpod.server;

import com.wi.floatingpod.config.SourceLimitProfile;
import java.io.IOException;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import static com.wi.floatingpod.server.B1HandoffTransferDataCodec.*;

/** 完整 pre-plan transfer intent DATA；不解析 terminal 为 KNC，不发 native/SQL/custody 资格。 */
public final class B1HandoffTransferIntentCodec {
    public static final String DOMAIN = "FP_B1_HANDOFF_PREPLAN_TRANSFER_BE_V4";
    public static final String OPERATION = "OWNER_TRANSFER_ONLY_NO_SQL";
    public static final String PREFIX_PROTOCOL = "EXACT_OWNER_PREFIX_LARGEST_K_SUFFIX_ONLY_V1";
    private static final String KEY_DOMAIN = "FP_B1_HANDOFF_PLAN_KEY_BE_V1";
    private B1HandoffTransferIntentCodec() {}

    public static final class NativeArtifact {
        private final Reference reference;
        private final byte[] body;
        public NativeArtifact(Reference reference, byte[] body) throws IOException {
            this.reference = Objects.requireNonNull(reference); reference.requireBody(body); this.body = body.clone();
        }
        public Reference reference() { return reference; }
        public byte[] body() { return body.clone(); }
    }
    public static final class Inventory {
        private final SourceLimitProfile profile;
        private final byte[] sourceAnchor, bridgeBytes, currentBytes, inactiveBytes, receiptBytes;
        private final WindowsMarkerIo.FileIdentity directory, guard, receipt, bridgeIdentity, currentIdentity, inactiveIdentity;
        private final String nativePath, namespace, baseKey;
        private final B1ResumeRecord bridge;
        private final B1ResumeRecordV5 current, inactive;
        private final B1HandoffArtifactClosure closure;
        private final List<B1Full20WitnessReference> retainedHistory;
        public Inventory(SourceLimitProfile profile, byte[] sourceAnchor,
                WindowsMarkerIo.FileIdentity directory, WindowsMarkerIo.FileIdentity guard,
                WindowsMarkerIo.FileIdentity receipt, String nativePath,
                byte[] bridgeBytes, WindowsMarkerIo.FileIdentity bridgeIdentity,
                byte[] currentBytes, WindowsMarkerIo.FileIdentity currentIdentity,
                byte[] inactiveBytes, WindowsMarkerIo.FileIdentity inactiveIdentity,
                List<B1Full20WitnessReference> retainedHistory,
                List<B1HandoffArtifactClosure.Artifact> completeArtifacts) throws IOException {
            this.profile = Objects.requireNonNull(profile);
            if (sourceAnchor == null || sourceAnchor.length != 32 || nativePath == null ||
                nativePath.isEmpty() || nativePath.length() > 32766 || nativePath.indexOf((char) 0) >= 0)
                throw new IOException("Complete exact source anchor/native path DATA required");
            this.sourceAnchor = sourceAnchor.clone();
            this.directory = Objects.requireNonNull(directory); this.guard = Objects.requireNonNull(guard);
            this.receipt = Objects.requireNonNull(receipt); utf8(nativePath, 131064); this.nativePath = nativePath;
            this.bridgeIdentity = Objects.requireNonNull(bridgeIdentity);
            this.currentIdentity = Objects.requireNonNull(currentIdentity); this.inactiveIdentity = Objects.requireNonNull(inactiveIdentity);
            if (currentIdentity.equals(inactiveIdentity) || currentIdentity.equals(bridgeIdentity) ||
                inactiveIdentity.equals(bridgeIdentity)) throw new IOException("Distinct original artifact identities required");
            this.bridgeBytes = copyRecord(bridgeBytes); this.currentBytes = copyRecord(currentBytes); this.inactiveBytes = copyRecord(inactiveBytes);
            bridge = B1ResumeRecordCanonical.decodeData(this.bridgeBytes);
            current = B1ResumeRecordV5.decodeData(this.currentBytes); inactive = B1ResumeRecordV5.decodeData(this.inactiveBytes);
            receiptBytes = bridge.originalReceipt(); namespace = bridge.barrierNamespace();
            var original = WindowsWorldProvisioner.decodeReceipt(receiptBytes);
            if (!original.sourceIdentity().equals(directory.versionedIdentity()) ||
                !original.guardIdentity().equals(guard.versionedIdentity()))
                throw new IOException("Original complete receipt differs from fixed source/guard identity");
            source(current.transition()); source(inactive.transition()); source(bridge);
            for (var v : List.of(current, inactive)) {
                if (v.bridgeSequence() != bridge.sequence() || !v.bridgeIdentity().equals(bridgeIdentity) ||
                    !Arrays.equals(v.bridgeSha256(), digest(this.bridgeBytes)))
                    throw new IOException("Complete V5 bridge bytes/identity/sequence differ");
            }
            B1ResumeRecordV5.consecutive(inactive, this.inactiveBytes, current);
            if (inactive.transition().sequence() == Math.addExact(bridge.sequence(), 1)) {
                if (!Arrays.equals(inactive.previousContainerSha256(), digest(this.bridgeBytes)))
                    throw new IOException("First V5 container differs from original V4 predecessor");
                B1ResumeRecordCodec.consecutive(bridge, this.bridgeBytes, inactive.transition());
            }
            if (retainedHistory == null) throw new IOException("Explicit complete retained history required");
            var historyNames = new HashSet<String>();
            for (var ref : retainedHistory)
                if (ref == null || !historyNames.add(ref.basename()))
                    throw new IOException("Explicit retained history must be unique complete reference DATA");
            this.retainedHistory = List.copyOf(retainedHistory);
            closure = new B1HandoffArtifactClosure(profile, this.sourceAnchor, digest(receiptBytes),
                inactive, current, this.retainedHistory, completeArtifacts);
            baseKey = HexFormat.of().formatHex(digest(keyBytes(this)));
        }
        private void source(B1ResumeRecord record) throws IOException {
            if (!Arrays.equals(record.sourceAnchorSha256(), sourceAnchor) ||
                !Arrays.equals(record.originalReceipt(), receiptBytes) || !record.barrierNamespace().equals(namespace))
                throw new IOException("Complete original slot source/receipt/namespace differ");
        }
        public String baseKey() { return baseKey; }
        public SourceLimitProfile profile() { return profile; }
        public B1HandoffArtifactClosure closure() { return closure; }
        public byte[] bridgeBytes() { return bridgeBytes.clone(); }
        public byte[] currentBytes() { return currentBytes.clone(); }
        public byte[] inactiveBytes() { return inactiveBytes.clone(); }
        public byte[] receiptBytes() { return receiptBytes.clone(); }
    }
    /** 固定 old-g birth 的完整 predecessor DATA 组件，不是完整 executor-birth ACK/SQL 资格。 */
    public static final class OriginalBirthPredecessorData {
        public static final String DOMAIN = "FP_B1_HANDOFF_BIRTH_PREDECESSOR_DATA_BE_V1";
        private final Inventory predecessor;
        private final long generation;
        public OriginalBirthPredecessorData(Inventory predecessor, long generation) throws IOException {
            this.predecessor = Objects.requireNonNull(predecessor);
            if (generation < 0) throw new IOException("Fixed original birth generation required");
            this.generation = generation;
        }
        public Inventory predecessor() { return predecessor; }
        public long generation() { return generation; }
        public String baseKey() { return predecessor.baseKey; }
    }
    public static final class IntentData {
        private final OriginalBirthPredecessorData birth;
        private final Inventory inventory;
        private final long generation, transferGeneration;
        private final NativeArtifact oldTerminal, priorTransferSupersession;
        private final byte[] beforeOwner, targetOwner, retirementEvidence;
        private final String retirementEvidenceProtocol;
        public IntentData(OriginalBirthPredecessorData birth, Inventory inventory, long generation, long transferGeneration,
                NativeArtifact oldTerminal, NativeArtifact priorTransferSupersession,
                byte[] beforeOwner, byte[] targetOwner, String retirementEvidenceProtocol,
                byte[] retirementEvidence) throws IOException {
            this.birth = Objects.requireNonNull(birth);
            this.inventory = Objects.requireNonNull(inventory); this.generation = generation; this.transferGeneration = transferGeneration;
            if (birth.generation != generation) throw new IOException("Original birth generation differs from old g");
            requireOriginalPrefix(birth.predecessor, inventory);
            B1HandoffGenerationName.transfer(B1HandoffGenerationName.Family.TRANSFER, birth.baseKey(), generation, transferGeneration);
            this.oldTerminal = Objects.requireNonNull(oldTerminal);
            B1HandoffGenerationName.parse(oldTerminal.reference.basename()).requireCanonical(
                B1HandoffGenerationName.Family.TERMINAL, birth.baseKey(), generation, -1);
            if (transferGeneration == 0 && priorTransferSupersession != null ||
                transferGeneration > 0 && priorTransferSupersession == null)
                throw new IOException("Exact initial/prior transfer supersession topology required");
            if (priorTransferSupersession != null)
                B1HandoffGenerationName.parse(priorTransferSupersession.reference.basename()).requireCanonical(
                    B1HandoffGenerationName.Family.TRANSFER_SUPERSESSION, birth.baseKey(), generation, transferGeneration - 1);
            this.priorTransferSupersession = priorTransferSupersession;
            this.beforeOwner = checkedOwner(beforeOwner, inventory.directory, inventory.guard, digest(inventory.receiptBytes));
            this.targetOwner = checkedOwner(targetOwner, inventory.directory, inventory.guard, digest(inventory.receiptBytes));
            if (owner(this.beforeOwner).boot().equals(owner(this.targetOwner).boot()))
                throw new IOException("Independent target boot differs from original B owner");
            if (retirementEvidenceProtocol == null || retirementEvidenceProtocol.isEmpty() ||
                retirementEvidenceProtocol.length() > 128 || retirementEvidence == null || retirementEvidence.length == 0 ||
                retirementEvidence.length > inventory.profile.maxNativeArtifactBytes() ||
                oldTerminal.body.length > inventory.profile.maxNativeArtifactBytes() ||
                priorTransferSupersession != null && priorTransferSupersession.body.length > inventory.profile.maxNativeArtifactBytes())
                throw new IOException("Complete bounded original retirement/terminal DATA required");
            utf8(retirementEvidenceProtocol, 128); this.retirementEvidenceProtocol = retirementEvidenceProtocol;
            this.retirementEvidence = retirementEvidence.clone();
        }
        public Inventory inventory() { return inventory; }
        public OriginalBirthPredecessorData originalBirth() { return birth; }
        public String basename() {
            return B1HandoffGenerationName.transfer(B1HandoffGenerationName.Family.TRANSFER,
                birth.baseKey(), generation, transferGeneration).basename();
        }
        public byte[] beforeOwner() { return beforeOwner.clone(); }
        public byte[] targetOwner() { return targetOwner.clone(); }
        public byte[] retirementEvidence() { return retirementEvidence.clone(); }
        public NativeArtifact oldTerminal() { return oldTerminal; }
        /** 实际 publication 后的 DATA 对应；reference 不是 private ACK。 */
        public TargetResultData targetResult(Reference actualIntentReference) throws IOException {
            B1HandoffGenerationName.parse(actualIntentReference.basename()).requireCanonical(
                B1HandoffGenerationName.Family.TRANSFER, birth.baseKey(), generation, transferGeneration);
            actualIntentReference.requireBody(encode(this));
            return new TargetResultData(inventory.profile, inventory.sourceAnchor, digest(inventory.receiptBytes),
                inventory.namespace, birth.baseKey(), generation, transferGeneration,
                actualIntentReference, oldTerminal.reference, beforeOwner, targetOwner, inventory.directory, inventory.guard,
                inventory.receipt, fact(inventory.bridge.sequence(), inventory.bridgeIdentity, inventory.bridgeBytes),
                fact(inventory.current.transition().sequence(), inventory.currentIdentity, inventory.currentBytes),
                fact(inventory.inactive.transition().sequence(), inventory.inactiveIdentity, inventory.inactiveBytes),
                inventory.closure.references());
        }
    }
    public static byte[] encode(IntentData data) throws IOException {
        Objects.requireNonNull(data); var v = data.inventory;
        return B1HandoffTransferDataCodec.encode(v.profile, out -> {
            text(out, DOMAIN); out.writeInt(4); text(out, OPERATION); text(out, PREFIX_PROTOCOL);
            blob(out, v.profile.canonicalBytes()); out.write(v.sourceAnchor); blob(out, v.receiptBytes);
            text(out, v.namespace); blob(out, utf8(v.nativePath, 131064));
            identity(out, v.directory); identity(out, v.guard); identity(out, v.receipt);
            blob(out, encodeBirthPredecessor(data.birth));
            blob(out, keyBytes(v)); text(out, data.birth.baseKey()); out.writeLong(data.generation); out.writeLong(data.transferGeneration);
            nativeArtifact(out, data.oldTerminal);
            out.writeByte(data.priorTransferSupersession == null ? 0 : 1);
            if (data.priorTransferSupersession != null) nativeArtifact(out, data.priorTransferSupersession);
            blob(out, v.bridgeBytes); identity(out, v.bridgeIdentity);
            blob(out, v.currentBytes); identity(out, v.currentIdentity);
            blob(out, v.inactiveBytes); identity(out, v.inactiveIdentity);
            out.writeInt(v.retainedHistory.size());
            for (var ref : v.retainedHistory) preservedReference(out, ref);
            out.writeInt(v.closure.artifacts().size());
            for (var artifact : v.closure.artifacts()) {
                preservedReference(out, artifact.reference()); blob(out, artifact.body());
            }
            blob(out, data.beforeOwner); blob(out, data.targetOwner);
            text(out, data.retirementEvidenceProtocol); blob(out, data.retirementEvidence); out.writeInt(1);
        });
    }
    public static IntentData decode(byte[] input, SourceLimitProfile expectedProfile) throws IOException {
        try {
            var r = B1HandoffTransferDataCodec.reader(input, expectedProfile);
            r.tag(DOMAIN, 4, OPERATION);
            if (!r.text(128).equals(PREFIX_PROTOCOL)) throw new IOException("Unknown exact owner prefix protocol");
            SourceLimitProfile profile = SourceLimitProfile.decode(r.blob(97), expectedProfile.sha256());
            byte[] source = r.hash(), receiptBody = r.blob(65536);
            String namespace = r.text(65536), nativePath = r.text(131064);
            var directory = r.identity(); var guard = r.identity(); var receipt = r.identity();
            OriginalBirthPredecessorData birth = decodeBirthPredecessor(r.blob(artifactLimit(profile)), profile);
            byte[] keyBody = r.blob(65536); String key = r.text(64); long g = r.in.readLong(), t = r.in.readLong();
            var terminal = nativeArtifact(r, profile);
            int present = r.in.readUnsignedByte();
            if (present > 1) throw new IOException("Noncanonical prior transfer tag");
            var prior = present == 1 ? nativeArtifact(r, profile) : null;
            byte[] bridge = r.blob(65536); var bridgeId = r.identity();
            byte[] current = r.blob(65536); var currentId = r.identity();
            byte[] inactive = r.blob(65536); var inactiveId = r.identity();
            int count = boundedCount(r, profile);
            List<B1Full20WitnessReference> history = new ArrayList<>(count);
            for (int i = 0; i < count; i++) history.add(r.preservedReference());
            count = boundedCount(r, profile);
            List<B1HandoffArtifactClosure.Artifact> artifacts = new ArrayList<>(count);
            long cumulative = 0;
            for (int i = 0; i < count; i++) {
                var reference = r.preservedReference();
                byte[] body = r.blob(artifactLimit(profile));
                cumulative = Math.addExact(cumulative, body.length);
                if (cumulative > profile.maxRetainedBytes()) throw new IOException("Complete closure cumulative body exceeds profile");
                artifacts.add(new B1HandoffArtifactClosure.Artifact(reference, body));
            }
            byte[] b = r.blob(164), c = r.blob(164);
            String protocol = r.text(128); byte[] evidence = r.blob(artifactLimit(profile)); r.finish();
            var inventory = new Inventory(profile, source, directory, guard, receipt, nativePath,
                bridge, bridgeId, current, currentId, inactive, inactiveId, history, artifacts);
            if (!Arrays.equals(receiptBody, inventory.receiptBytes) || !namespace.equals(inventory.namespace) ||
                !key.equals(birth.baseKey()) || !Arrays.equals(keyBody, keyBytes(inventory)))
                throw new IOException("Exact source-bound key/receipt/namespace body differs");
            var data = new IntentData(birth, inventory, g, t, terminal, prior, b, c, protocol, evidence);
            if (!Arrays.equals(input, encode(data))) throw new IOException("Noncanonical full pre-plan intent");
            return data;
        } catch (IllegalArgumentException | ArithmeticException failure) {
            throw new IOException("Invalid complete source-bound pre-plan intent DATA", failure);
        }
    }
    /** 原组件 encode 保存完整 preimage 与 full-ref closure，不嵌自身 ref 或未来 result。 */
    public static byte[] encodeBirthPredecessor(OriginalBirthPredecessorData birth) throws IOException {
        return B1HandoffTransferDataCodec.encode(birth.predecessor.profile, out -> {
            text(out, OriginalBirthPredecessorData.DOMAIN); out.writeInt(1); out.writeLong(birth.generation);
            Inventory v = birth.predecessor;
            blob(out, v.profile.canonicalBytes()); out.write(v.sourceAnchor); blob(out, utf8(v.nativePath, 131064));
            identity(out, v.directory); identity(out, v.guard); identity(out, v.receipt);
            blob(out, v.bridgeBytes); identity(out, v.bridgeIdentity);
            blob(out, v.currentBytes); identity(out, v.currentIdentity);
            blob(out, v.inactiveBytes); identity(out, v.inactiveIdentity);
            out.writeInt(v.retainedHistory.size());
            for (var ref : v.retainedHistory) preservedReference(out, ref);
            out.writeInt(v.closure.artifacts().size());
            for (var artifact : v.closure.artifacts()) {
                preservedReference(out, artifact.reference()); blob(out, artifact.body());
            }
            blob(out, keyBytes(v)); text(out, v.baseKey); out.writeInt(1);
        });
    }
    public static OriginalBirthPredecessorData decodeBirthPredecessor(byte[] bytes, SourceLimitProfile expected) throws IOException {
        try {
            var r = B1HandoffTransferDataCodec.reader(bytes, expected);
            if (!r.text(128).equals(OriginalBirthPredecessorData.DOMAIN) || r.in.readInt() != 1)
                throw new IOException("Unknown original birth predecessor DATA domain");
            long g = r.in.readLong();
            SourceLimitProfile profile = SourceLimitProfile.decode(r.blob(97), expected.sha256());
            byte[] source = r.hash(); String nativePath = r.text(131064);
            var dir = r.identity(); var guard = r.identity(); var receipt = r.identity();
            byte[] bridge = r.blob(65536); var bridgeId = r.identity();
            byte[] current = r.blob(65536); var currentId = r.identity();
            byte[] inactive = r.blob(65536); var inactiveId = r.identity();
            int count = boundedCount(r, profile);
            List<B1Full20WitnessReference> history = new ArrayList<>(count);
            for (int i = 0; i < count; i++) history.add(r.preservedReference());
            count = boundedCount(r, profile);
            List<B1HandoffArtifactClosure.Artifact> artifacts = new ArrayList<>(count);
            for (int i = 0; i < count; i++) artifacts.add(
                new B1HandoffArtifactClosure.Artifact(r.preservedReference(), r.blob(artifactLimit(profile))));
            byte[] keyBody = r.blob(65536); String key = r.text(64); r.finish();
            var inventory = new Inventory(profile, source, dir, guard, receipt, nativePath,
                bridge, bridgeId, current, currentId, inactive, inactiveId, history, artifacts);
            if (!key.equals(inventory.baseKey) || !Arrays.equals(keyBody, keyBytes(inventory)))
                throw new IOException("Fixed original birth key differs from complete original predecessor preimage");
            var birth = new OriginalBirthPredecessorData(inventory, g);
            if (!Arrays.equals(bytes, encodeBirthPredecessor(birth))) throw new IOException("Noncanonical original birth predecessor DATA");
            return birth;
        } catch (IllegalArgumentException | ArithmeticException failure) {
            throw new IOException("Invalid complete original birth predecessor DATA", failure);
        }
    }
    private static void requireOriginalPrefix(Inventory original, Inventory observed) throws IOException {
        if (!original.profile.equals(observed.profile) || !original.directory.equals(observed.directory) ||
            !original.guard.equals(observed.guard) || !original.receipt.equals(observed.receipt) ||
            !original.nativePath.equals(observed.nativePath) || !Arrays.equals(original.sourceAnchor, observed.sourceAnchor) ||
            !Arrays.equals(original.receiptBytes, observed.receiptBytes) ||
            !original.bridgeIdentity.equals(observed.bridgeIdentity) || !Arrays.equals(original.bridgeBytes, observed.bridgeBytes))
            throw new IOException("Observed prefix differs from complete original birth source/bridge lineage");
        long originalSeq = original.current.transition().sequence();
        long observedSeq = observed.current.transition().sequence();
        if (observedSeq == originalSeq) {
            if (!original.currentIdentity.equals(observed.currentIdentity) ||
                !original.inactiveIdentity.equals(observed.inactiveIdentity) ||
                !Arrays.equals(original.currentBytes, observed.currentBytes) ||
                !Arrays.equals(original.inactiveBytes, observed.inactiveBytes))
                throw new IOException("Unadvanced prefix changed original complete slots/identities");
        } else if (observedSeq == Math.addExact(originalSeq, 1)) {
            if (!original.currentIdentity.equals(observed.inactiveIdentity) ||
                !Arrays.equals(original.currentBytes, observed.inactiveBytes))
                throw new IOException("Advanced prefix lost original retained current bytes/identity");
            B1ResumeRecordV5.consecutive(original.current, original.currentBytes, observed.current);
        } else throw new IOException("Missing/conflicting complete original single-publication lineage");
    }
    private static byte[] keyBytes(Inventory v) throws IOException {
        return B1HandoffTransferDataCodec.encode(v.profile, out -> {
            text(out, KEY_DOMAIN); out.write(v.sourceAnchor); out.write(v.profile.sha256()); out.write(digest(v.receiptBytes));
            text(out, v.namespace); out.writeLong(v.bridge.sequence()); identity(out, v.bridgeIdentity); out.write(digest(v.bridgeBytes));
            out.writeLong(v.current.transition().sequence()); out.writeInt((int) (v.current.transition().sequence() % 2));
            identity(out, v.currentIdentity); out.write(digest(v.currentBytes)); out.writeInt(1);
            out.writeLong(v.inactive.transition().sequence()); out.writeInt((int) (v.inactive.transition().sequence() % 2));
            identity(out, v.inactiveIdentity); out.write(digest(v.inactiveBytes)); out.writeInt(1);
        });
    }
    private static StateFact fact(long sequence, WindowsMarkerIo.FileIdentity id, byte[] body) {
        return new StateFact(sequence, (int) (sequence % 2), id, digest(body));
    }
    private static void nativeArtifact(DataOutputStream out, NativeArtifact a) throws IOException {
        reference(out, a.reference); blob(out, a.body);
    }
    private static NativeArtifact nativeArtifact(Reader r, SourceLimitProfile profile) throws IOException {
        return new NativeArtifact(r.reference(), r.blob(artifactLimit(profile)));
    }
    private static int artifactLimit(SourceLimitProfile profile) {
        return (int) Math.min(profile.maxNativeArtifactBytes(), Integer.MAX_VALUE - 8L);
    }
    private static int boundedCount(Reader r, SourceLimitProfile p) throws IOException {
        int count = r.in.readInt();
        if (count < 0 || count > p.maxMembers() || count > r.in.available() / 69)
            throw new IOException("Invalid original complete closure count");
        return count;
    }
    private static byte[] copyRecord(byte[] body) throws IOException {
        if (body == null || body.length <= 0 || body.length > 65536) throw new IOException("Complete original bounded slot body required");
        return body.clone();
    }
}
