package com.wi.floatingpod.server;

import com.wi.floatingpod.config.SourceLimitProfile;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * METHOD4 固定 target-result/retirement DATA codec。decoded DATA 不签 intent/ACK/退休/SQL。
 * 实际调用者在同 registered root 预留 workspaceBound 后才调用；此纯依赖不拥有 root 或 HANDLE。
 */
public final class B1HandoffTransferDataCodec {
    public static final String RESULT_DOMAIN = "FP_B1_HANDOFF_TRANSFER_TARGET_RESULT_BE_V3";
    public static final String RESULT_TAG = "TRANSFER_TARGET_BYTES_DURABLE";
    public static final String RETIREMENT_DOMAIN = "FP_B1_HANDOFF_TRANSFER_TARGET_RETIREMENT_BE_V4";
    public static final String RETIREMENT_TAG = "TRANSFER_TARGET_RETIRED_NO_SQL_AUTHORITY";
    public static final String RETIREMENT_PROTOCOL =
        "EXACT_TARGET_LIFETIME_AND_SOURCE_WIDE_LATE_WRITER_EXCLUSION_V1";
    private B1HandoffTransferDataCodec() {}

    public static long workspaceBound(long fullLength) {
        if (fullLength <= 0 || fullLength > Integer.MAX_VALUE - 8)
            throw new IllegalArgumentException("Positive JVM artifact length required");
        return Math.addExact(Math.multiplyExact(fullLength, 32L), 8192L);
    }
    /** 公开 immutable 磁盘 DATA 引用；相同字段不等于 native durability。 */
    public static final class Reference {
        private final String basename;
        private final WindowsMarkerIo.FileIdentity identity;
        private final long length;
        private final byte[] sha256;
        public Reference(String basename, WindowsMarkerIo.FileIdentity identity, long length, byte[] sha256) {
            if (basename == null || !basename.startsWith(".floatingpod-") || basename.length() > 255 ||
                basename.endsWith(".tmp") || length <= 0)
                throw new IllegalArgumentException("Canonical complete artifact reference required");
            for (int i = 0; i < basename.length(); i++) {
                char c = basename.charAt(i);
                if (!(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '.' || c == '-'))
                    throw new IllegalArgumentException("Fixed ASCII artifact basename required");
            }
            this.basename = basename; this.identity = Objects.requireNonNull(identity);
            this.length = length; this.sha256 = hash(sha256);
            try { if (B1HandoffGenerationName.parse(basename).isTemporary())
                throw new IllegalArgumentException("Canonical HANDOFF reference required"); }
            catch (IOException failure) { throw new IllegalArgumentException("Versioned HANDOFF reference name required", failure); }
        }
        public String basename() { return basename; }
        public WindowsMarkerIo.FileIdentity identity() { return identity; }
        public long length() { return length; }
        public byte[] sha256() { return sha256.clone(); }
        public void requireBody(byte[] fullBody) throws IOException {
            if (fullBody == null || fullBody.length != length || !MessageDigest.isEqual(sha256, digest(fullBody)))
                throw new IOException("Complete artifact body differs from actual reference DATA");
        }
    }
    /** seq/slot/identity/full-body hash 均来自完整原 inventory；不证明当前存在。 */
    public static final class StateFact {
        private final long sequence;
        private final int slot;
        private final WindowsMarkerIo.FileIdentity identity;
        private final byte[] sha256;
        public StateFact(long sequence, int slot, WindowsMarkerIo.FileIdentity identity, byte[] sha256) {
            if (sequence < 0 || slot != sequence % 2) throw new IllegalArgumentException("Exact seq parity slot required");
            this.sequence = sequence; this.slot = slot; this.identity = Objects.requireNonNull(identity);
            this.sha256 = hash(sha256);
        }
        public long sequence() { return sequence; }
        public int slot() { return slot; }
        public WindowsMarkerIo.FileIdentity identity() { return identity; }
        public byte[] sha256() { return sha256.clone(); }
    }
    /** 不接受 live/retired/observer 参数，所以完整 intent 的 R(I) 不随 C 生命周期变化。 */
    public static final class TargetResultData {
        private final SourceLimitProfile profile;
        private final byte[] sourceAnchor, receiptSha256, beforeOwner, targetOwner;
        private final String namespace, baseKey;
        private final long generation, transferGeneration;
        private final Reference intent, oldTerminal;
        private final WindowsMarkerIo.FileIdentity directory, guard, receipt;
        private final StateFact bridge, current, inactive;
        private final List<B1Full20WitnessReference> preserved;
        public TargetResultData(SourceLimitProfile profile, byte[] sourceAnchor, byte[] receiptSha256,
                String namespace, String baseKey, long generation, long transferGeneration,
                Reference intent, Reference oldTerminal, byte[] beforeOwner, byte[] targetOwner,
                WindowsMarkerIo.FileIdentity directory, WindowsMarkerIo.FileIdentity guard,
                WindowsMarkerIo.FileIdentity receipt, StateFact bridge, StateFact current, StateFact inactive,
                List<B1Full20WitnessReference> preserved) throws IOException {
            this.profile = Objects.requireNonNull(profile);
            this.sourceAnchor = hash(sourceAnchor); this.receiptSha256 = hash(receiptSha256);
            utf8(namespace); this.namespace = namespace;
            B1HandoffGenerationName.transfer(B1HandoffGenerationName.Family.TRANSFER, baseKey, generation, transferGeneration);
            this.baseKey = baseKey; this.generation = generation; this.transferGeneration = transferGeneration;
            this.intent = Objects.requireNonNull(intent); this.oldTerminal = Objects.requireNonNull(oldTerminal);
            B1HandoffGenerationName.parse(intent.basename).requireCanonical(
                B1HandoffGenerationName.Family.TRANSFER, baseKey, generation, transferGeneration);
            B1HandoffGenerationName.parse(oldTerminal.basename).requireCanonical(
                B1HandoffGenerationName.Family.TERMINAL, baseKey, generation, -1);
            this.directory = Objects.requireNonNull(directory); this.guard = Objects.requireNonNull(guard);
            this.receipt = Objects.requireNonNull(receipt);
            this.beforeOwner = checkedOwner(beforeOwner, directory, guard, this.receiptSha256);
            this.targetOwner = checkedOwner(targetOwner, directory, guard, this.receiptSha256);
            var old = owner(this.beforeOwner); var target = owner(this.targetOwner);
            if (old.boot().equals(target.boot())) throw new IOException("Original B and target C boot must differ");
            this.bridge = Objects.requireNonNull(bridge); this.current = Objects.requireNonNull(current);
            this.inactive = Objects.requireNonNull(inactive);
            if (Math.addExact(inactive.sequence, 1) != current.sequence || bridge.sequence > inactive.sequence ||
                current.slot == inactive.slot)
                throw new IOException("Complete original occupied inventory sequence differs");
            if (preserved == null || preserved.size() > profile.maxMembers())
                throw new IOException("Preserved reference count exceeds original profile");
            this.preserved = List.copyOf(preserved);
            var names = new HashSet<String>();
            var attempts = new HashMap<UUID, B1Full20WitnessReference>();
            for (B1Full20WitnessReference ref : this.preserved) {
                if (ref == null || !names.add(ref.basename()) || ref.fullLength() > profile.maxNativeArtifactBytes() ||
                    !Arrays.equals(ref.sourceAnchorSha256(), this.sourceAnchor) ||
                    !Arrays.equals(ref.originalReceiptSha256(), this.receiptSha256) ||
                    !Arrays.equals(ref.profileSha256(), profile.sha256()))
                    throw new IOException("Duplicate/invalid preserved artifact reference");
                var previous = attempts.putIfAbsent(ref.attemptId(), ref);
                if (previous != null && !previous.exactlyEquals(ref))
                    throw new IOException("Original attempt has conflicting preserved reference DATA");
            }
            if (intent.length > profile.maxNativeArtifactBytes() || oldTerminal.length > profile.maxNativeArtifactBytes())
                throw new IOException("Original reference length exceeds artifact profile");
        }
        public SourceLimitProfile profile() { return profile; }
        public String baseKey() { return baseKey; }
        public long generation() { return generation; }
        public long transferGeneration() { return transferGeneration; }
        public byte[] beforeOwner() { return beforeOwner.clone(); }
        public byte[] targetOwner() { return targetOwner.clone(); }
        public Reference intent() { return intent; }
        public Reference oldTerminal() { return oldTerminal; }
        public List<B1Full20WitnessReference> preserved() { return preserved; }
        public String basename() {
            return B1HandoffGenerationName.transfer(B1HandoffGenerationName.Family.TARGET_RESULT,
                baseKey, generation, transferGeneration).basename();
        }
    }
    public static final class RetirementData {
        private final TargetResultData original;
        private final Reference result;
        /** 校验完整 result/reference DATA 对应，不发行 C 已退休资格。 */
        public RetirementData(TargetResultData original, Reference actualResult) throws IOException {
            this.original = Objects.requireNonNull(original); this.result = Objects.requireNonNull(actualResult);
            B1HandoffGenerationName.parse(actualResult.basename).requireCanonical(
                B1HandoffGenerationName.Family.TARGET_RESULT, original.baseKey, original.generation, original.transferGeneration);
            actualResult.requireBody(encodeTargetResult(original));
        }
        public TargetResultData original() { return original; }
        public Reference result() { return result; }
        public String basename() {
            return B1HandoffGenerationName.transfer(B1HandoffGenerationName.Family.TARGET_RETIREMENT,
                original.baseKey, original.generation, original.transferGeneration).basename();
        }
    }
    public static byte[] encodeTargetResult(TargetResultData data) throws IOException {
        Objects.requireNonNull(data);
        return encode(data.profile, out -> {
            text(out, RESULT_DOMAIN); out.writeInt(3); text(out, RESULT_TAG);
            common(out, data);
            // 固定 guard durable fact：原 identity + complete164 原 C bytes，没有 observer field。
            identity(out, data.guard); out.writeInt(WindowsWorldProvisioner.OWNER_SIZE); blob(out, data.targetOwner);
            out.writeInt(1);
        });
    }
    public static byte[] encodeRetirement(RetirementData data) throws IOException {
        Objects.requireNonNull(data);
        return encode(data.original.profile, out -> {
            text(out, RETIREMENT_DOMAIN); out.writeInt(4); text(out, RETIREMENT_TAG);
            common(out, data.original); reference(out, data.result);
            B1ResumeRecord.NativeOwnerAssertions c = owner(data.original.targetOwner);
            out.writeLong(c.pid()); out.writeLong(c.fileTime()); uuid(out, c.boot()); out.writeLong(c.generation());
            identity(out, data.original.guard); text(out, RETIREMENT_PROTOCOL); out.writeInt(1);
        });
    }
    public static TargetResultData decodeTargetResult(byte[] input, SourceLimitProfile expectedProfile) throws IOException {
        try {
            Reader reader = reader(input, expectedProfile);
            reader.tag(RESULT_DOMAIN, 3, RESULT_TAG);
            TargetResultData data = reader.common(expectedProfile);
            if (!reader.identity().equals(data.guard) || reader.in.readInt() != WindowsWorldProvisioner.OWNER_SIZE ||
                !Arrays.equals(reader.blob(WindowsWorldProvisioner.OWNER_SIZE), data.targetOwner))
                throw new IOException("Fixed target guard fact differs from original C recipe");
            reader.finish();
            if (!Arrays.equals(input, encodeTargetResult(data))) throw new IOException("Noncanonical target result");
            return data;
        } catch (IllegalArgumentException | ArithmeticException failure) {
            throw new IOException("Invalid complete target-result DATA", failure);
        }
    }
    public static RetirementData decodeRetirement(byte[] input, SourceLimitProfile expectedProfile) throws IOException {
        try {
            Reader reader = reader(input, expectedProfile);
            reader.tag(RETIREMENT_DOMAIN, 4, RETIREMENT_TAG);
            TargetResultData original = reader.common(expectedProfile);
            RetirementData data = new RetirementData(original, reader.reference());
            var c = owner(original.targetOwner);
            if (reader.in.readLong() != c.pid() || reader.in.readLong() != c.fileTime() ||
                !reader.uuid().equals(c.boot()) || reader.in.readLong() != c.generation() ||
                !reader.identity().equals(original.guard) || !reader.text(128).equals(RETIREMENT_PROTOCOL))
                throw new IOException("Retirement fixed original target/protocol differs");
            reader.finish();
            if (!Arrays.equals(input, encodeRetirement(data))) throw new IOException("Noncanonical target retirement");
            return data;
        } catch (IllegalArgumentException | ArithmeticException failure) {
            throw new IOException("Invalid complete target-retirement DATA", failure);
        }
    }
    private static void common(DataOutputStream out, TargetResultData d) throws IOException {
        blob(out, d.profile.canonicalBytes()); out.write(d.sourceAnchor); out.write(d.receiptSha256);
        text(out, d.namespace); text(out, d.baseKey); out.writeLong(d.generation); out.writeLong(d.transferGeneration);
        reference(out, d.intent); reference(out, d.oldTerminal);
        blob(out, d.beforeOwner); out.write(digest(d.beforeOwner));
        blob(out, d.targetOwner); out.write(digest(d.targetOwner));
        identity(out, d.directory); identity(out, d.guard); identity(out, d.receipt);
        fact(out, d.bridge); fact(out, d.current); fact(out, d.inactive);
        out.writeInt(d.preserved.size());
        for (B1Full20WitnessReference ref : d.preserved) preservedReference(out, ref);
    }
    static Reader reader(byte[] input, SourceLimitProfile profile) throws IOException {
        Objects.requireNonNull(profile);
        if (input == null || input.length <= 0 || input.length > profile.maxNativeArtifactBytes() ||
            workspaceBound(input.length) > profile.maxRetainedBytes())
            throw new IOException("Complete DATA codec workspace exceeds original profile");
        return new Reader(input);
    }
    static final class Reader {
        final DataInputStream in;
        Reader(byte[] bytes) { in = new DataInputStream(new ByteArrayInputStream(bytes)); }
        byte[] blob(int limit) throws IOException {
            int length = in.readInt();
            if (length <= 0 || length > limit || length > in.available())
                throw new IOException("Invalid bounded complete DATA blob length");
            byte[] result = new byte[length]; in.readFully(result); return result;
        }
        byte[] hash() throws IOException { byte[] b = new byte[32]; in.readFully(b); return b; }
        String text(int limit) throws IOException {
            try {
                return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(blob(limit))).toString();
            } catch (CharacterCodingException failure) { throw new IOException("Malformed canonical UTF8 DATA", failure); }
        }
        void tag(String domain, int version, String tag) throws IOException {
            if (!text(128).equals(domain) || in.readInt() != version || !text(128).equals(tag))
                throw new IOException("Unsupported DATA domain/version/tag");
        }
        WindowsMarkerIo.FileIdentity identity() throws IOException {
            long volume = in.readLong(); byte[] id = new byte[16]; in.readFully(id);
            return new WindowsMarkerIo.FileIdentity(volume, id);
        }
        UUID uuid() throws IOException { return new UUID(in.readLong(), in.readLong()); }
        Reference reference() throws IOException { return new Reference(text(255), identity(), in.readLong(), hash()); }
        StateFact fact() throws IOException { return new StateFact(in.readLong(), in.readInt(), identity(), hash()); }
        B1Full20WitnessReference preservedReference() throws IOException {
            String name = text(255); var id = identity(); long length = in.readLong();
            byte[] full = hash(), source = hash(), receipt = hash(), profile = hash();
            UUID attempt = uuid();
            String phaseName = text(32);
            B1ResumeRecord.Phase phase;
            try { phase = B1ResumeRecord.Phase.valueOf(phaseName); }
            catch (IllegalArgumentException failure) { throw new IOException("Unknown original reference phase", failure); }
            return new B1Full20WitnessReference(name, id, length, full, source, receipt, profile, attempt, phase, uuid());
        }
        TargetResultData common(SourceLimitProfile expected) throws IOException {
            SourceLimitProfile profile = SourceLimitProfile.decode(blob(97), expected.sha256());
            if (!profile.equals(expected)) throw new IOException("Complete original profile differs");
            byte[] source = hash(), receiptHash = hash();
            String namespace = text(65536), key = text(64);
            long g = in.readLong(), t = in.readLong(); Reference intent = reference(), terminal = reference();
            byte[] before = blob(164), beforeHash = hash(), target = blob(164), targetHash = hash();
            if (!MessageDigest.isEqual(beforeHash, digest(before)) || !MessageDigest.isEqual(targetHash, digest(target)))
                throw new IOException("Complete owner DATA hash differs");
            var directory = identity(); var guard = identity(); var receipt = identity();
            StateFact bridge = fact(), current = fact(), inactive = fact();
            int count = in.readInt();
            // 每 ref 至少 name length4+name1+identity24+length8+hash32=69，先拒绝不可容纳的 count。
            if (count < 0 || count > profile.maxMembers() || count > in.available() / 69)
                throw new IOException("Invalid bounded preserved reference count");
            List<B1Full20WitnessReference> refs = new ArrayList<>(count);
            for (int i = 0; i < count; i++) refs.add(preservedReference());
            return new TargetResultData(profile, source, receiptHash, namespace, key, g, t, intent, terminal,
                before, target, directory, guard, receipt, bridge, current, inactive, refs);
        }
        void finish() throws IOException {
            if (in.readInt() != 1 || in.read() != -1) throw new IOException("Invalid terminal/trailing DATA bytes");
        }
    }
    interface Writer { void write(DataOutputStream out) throws IOException; }
    static byte[] encode(SourceLimitProfile profile, Writer writer) throws IOException {
        Counter counter = new Counter(); writer.write(new DataOutputStream(counter));
        if (counter.length <= 0 || counter.length > profile.maxNativeArtifactBytes() ||
            counter.length > Integer.MAX_VALUE - 8 || workspaceBound(counter.length) > profile.maxRetainedBytes())
            throw new IOException("Complete DATA codec workspace exceeds original profile");
        Exact sink = new Exact((int) counter.length);
        writer.write(new DataOutputStream(sink)); return sink.finish();
    }
    private static final class Counter extends OutputStream {
        long length;
        @Override public void write(int ignored) { length = Math.addExact(length, 1); }
        @Override public void write(byte[] bytes, int start, int count) {
            Objects.checkFromIndexSize(start, count, bytes.length); length = Math.addExact(length, count);
        }
    }
    private static final class Exact extends OutputStream {
        private final byte[] bytes; private int offset;
        Exact(int length) { bytes = new byte[length]; }
        @Override public void write(int value) throws IOException {
            if (offset == bytes.length) throw new IOException("Exact DATA writer overrun");
            bytes[offset++] = (byte) value;
        }
        @Override public void write(byte[] value, int start, int count) throws IOException {
            Objects.checkFromIndexSize(start, count, value.length);
            if (count > bytes.length - offset) throw new IOException("Exact DATA writer overrun");
            System.arraycopy(value, start, bytes, offset, count); offset += count;
        }
        byte[] finish() throws IOException {
            if (offset != bytes.length) throw new IOException("Exact DATA writer underrun");
            return bytes;
        }
    }
    static void blob(DataOutputStream out, byte[] bytes) throws IOException {
        out.writeInt(bytes.length); out.write(bytes);
    }
    static void text(DataOutputStream out, String value) throws IOException { blob(out, utf8(value)); }
    static byte[] utf8(String text) throws IOException {
        return utf8(text, 65536);
    }
    static byte[] utf8(String text, int explicitLimit) throws IOException {
        if (explicitLimit <= 0 || text == null || text.isEmpty()) throw new IOException("Complete nonempty DATA text required");
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).encode(java.nio.CharBuffer.wrap(text));
            if (encoded.remaining() > explicitLimit) throw new IOException("DATA text exceeds explicit text bound");
            byte[] bytes = new byte[encoded.remaining()]; encoded.get(bytes); return bytes;
        } catch (CharacterCodingException failure) { throw new IOException("Malformed canonical DATA text", failure); }
    }
    static void reference(DataOutputStream out, Reference r) throws IOException {
        text(out, r.basename); identity(out, r.identity); out.writeLong(r.length); out.write(r.sha256);
    }
    static void preservedReference(DataOutputStream out, B1Full20WitnessReference r) throws IOException {
        text(out, r.basename()); identity(out, r.fileIdentity()); out.writeLong(r.fullLength());
        out.write(r.fullSha256()); out.write(r.sourceAnchorSha256()); out.write(r.originalReceiptSha256());
        out.write(r.profileSha256()); uuid(out, r.attemptId()); text(out, r.phase().name()); uuid(out, r.logicalOperation());
    }
    private static void fact(DataOutputStream out, StateFact f) throws IOException {
        out.writeLong(f.sequence); out.writeInt(f.slot); identity(out, f.identity); out.write(f.sha256);
    }
    static void identity(DataOutputStream out, WindowsMarkerIo.FileIdentity id) throws IOException {
        out.writeLong(id.volumeSerialNumber()); out.write(id.fileId128());
    }
    private static void uuid(DataOutputStream out, UUID id) throws IOException {
        out.writeLong(id.getMostSignificantBits()); out.writeLong(id.getLeastSignificantBits());
    }
    private static byte[] hash(byte[] value) {
        if (value == null || value.length != 32) throw new IllegalArgumentException("Exact SHA256 DATA required");
        return value.clone();
    }
    static byte[] digest(byte[] bytes) {
        try { return MessageDigest.getInstance("SHA-256").digest(bytes); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException("Required SHA256 unavailable", impossible); }
    }
    static byte[] checkedOwner(byte[] bytes, WindowsMarkerIo.FileIdentity directory,
            WindowsMarkerIo.FileIdentity guard, byte[] receiptHash) throws IOException {
        if (bytes == null || bytes.length != WindowsWorldProvisioner.OWNER_SIZE ||
            !MessageDigest.isEqual(Arrays.copyOfRange(bytes, 132, 164), digest(Arrays.copyOf(bytes, 132))))
            throw new IOException("Invalid complete164 owner DATA digest");
        ByteBuffer b = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        if (b.getLong() != WindowsWorldProvisioner.GUARD_MAGIC || b.getInt() != 1 ||
            !readIdentity(b).equals(directory) || !readIdentity(b).equals(guard))
            throw new IOException("Original owner DATA source/guard/version differs");
        byte[] actualReceipt = new byte[32]; b.get(actualReceipt);
        if (!MessageDigest.isEqual(actualReceipt, receiptHash)) throw new IOException("Original owner receipt differs");
        owner(bytes); return bytes.clone();
    }
    private static WindowsMarkerIo.FileIdentity readIdentity(ByteBuffer b) {
        long volume = b.getLong(); byte[] id = new byte[16]; b.get(id);
        return new WindowsMarkerIo.FileIdentity(volume, id);
    }
    static B1ResumeRecord.NativeOwnerAssertions owner(byte[] bytes) {
        ByteBuffer b = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN); b.position(92);
        return new B1ResumeRecord.NativeOwnerAssertions(b.getLong(), b.getLong(),
            new UUID(b.getLong(), b.getLong()), b.getLong());
    }
}
